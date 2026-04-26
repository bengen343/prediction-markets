-- Detect unusually large trades in the last 30 minutes and insert them into
-- the alerts table. Runs every 15 minutes via BQ scheduled query.
--
-- Threshold: size OR notional value must have a Z-score greater than 3,
-- with a floor of size >= 10. Requires at least 10 trades
-- of history per market; new markets won't trigger alerts until they
-- accumulate baseline volume, which is the intended behavior (no meaningful
-- baseline to compare against yet).

-- Compact human-friendly dollar formatting for the reason string:
--   850          -> "$850"
--   12_345       -> "$12.3K"
--   1_234_567    -> "$1.2M"
--   1_234_567_890 -> "$1.2B"
create temp function format_volume(v float64) as (
    case
        when v is null then '$0'
        when abs(v) >= 1e9 then format('$%.1fB', v / 1e9)
        when abs(v) >= 1e6 then format('$%.1fM', v / 1e6)
        when abs(v) >= 1e3 then format('$%.1fK', v / 1e3)
        else format('$%.0f', v)
    end
);

insert into prediction_markets.alerts (
    alert_id,
    series_ticker,
    series_title,
    source,
    title,
    market_id,
    trade_id,
    trade_ts,
    side,
    price,
    size,
    notional,
    size_zscore,
    notional_zscore,
    volume,
    volume_24h,
    reason
)
with recent_trades as (
    select
        source,
        market_id,
        trade_id,
        datetime(ts, 'America/Denver') as trade_ts,
        price,
        size,
        side,
        price * size as notional
    from prediction_markets.trades
    where ts > timestamp_sub(current_timestamp(), interval 30 minute)
        and size is not null
        and price is not null
),

rolling_stats as (
    select
        market_id,
        -- cast the size median to float64
        -- so downstream format() with %.1f accepts it (bq won't auto-coerce).
        cast(avg(size) as float64) as average_size,
        cast(stddev_samp(size) as float64) as stddev_size,
        cast(avg(price * size) as float64) as average_notional,
        cast(stddev_samp(price * size) as float64) as stddev_notional
    from prediction_markets.trades
    where ts > timestamp_sub(current_timestamp(), interval 7 day)
        and size is not null
        and price is not null
    group by market_id
    having count(*) >= 10
),

candidates as (
    select
        -- trade information
        to_hex(sha256(concat(rt.source, '|', rt.trade_id))) as alert_id,
        rt.source,
        rt.market_id,
        rt.trade_id,
        rt.trade_ts,
        rt.side,
        rt.price,
        rt.size,
        rt.notional,
        -- size metrics
        rs.average_size,
        rs.stddev_size,
        (rt.size - rs.average_size) / nullif(rs.stddev_size, 0) as size_zscore,
        -- notional value metrics
        rs.average_notional,
        rs.stddev_notional,
        (rt.notional - rs.average_notional) / nullif(rs.stddev_notional, 0) as notional_zscore
    from recent_trades rt
    join rolling_stats rs
      on rt.market_id = rs.market_id
    -- filter candidates to only those three standard devitions outside the mean
    where
      rt.notional > 1000
      and abs((rt.size - rs.average_size) / nullif(rs.stddev_size, 0)) > 3
      and abs((rt.notional - rs.average_notional) / nullif(rs.stddev_notional, 0)) > 3
),

alerts as (
    select
        -- alert information
        candidates.alert_id,
        -- series information
        coalesce(kalshi_markets.series_ticker, polymarket_markets.event_ticker) as series_ticker,
        coalesce(kalshi_series.title, polymarket_markets.event_title) as series_title,
        -- trade information
        candidates.source,
        coalesce(kalshi_markets.title, initcap(initcap(replace(polymarket_markets.slug, '-', ' ')) || '?')) as title,
        candidates.market_id,
        candidates.trade_id,
        candidates.trade_ts,
        candidates.side,
        candidates.price,
        candidates.size,
        candidates.notional,
        -- trade metrics
        candidates.size_zscore,
        candidates.notional_zscore,
        -- market information
        coalesce(kalshi_markets.volume_fp * 100, polymarket_markets.volume) as volume,
        coalesce(kalshi_markets.volume_24h_fp * 100, polymarket_markets.volume_24h) as volume_24h,
        -- alert
        -- ifnull guards: format() propagates NULL if any arg is NULL, which
        -- would NULL out the entire reason. z-scores can be NULL when
        -- stddev=0 on the *other* metric (the OR-trigger lets a row through
        -- even if its own z is NULL); volumes can be NULL on Kalshi markets
        -- without a /markets row yet.
        format(
            'size=%.2f (z=%.2f vs avg %.1f), notional=$%.2f (z=%.2f vs avg $%.2f), market vol=%s (24h %s)',
            candidates.size,
            ifnull(candidates.size_zscore, 0),
            candidates.average_size,
            candidates.notional,
            ifnull(candidates.notional_zscore, 0),
            candidates.average_notional,
            format_volume(coalesce(kalshi_markets.volume_fp * 100, polymarket_markets.volume)),
            format_volume(coalesce(kalshi_markets.volume_24h_fp * 100, polymarket_markets.volume_24h))
        ) as reason

    from candidates
    left join prediction_markets.markets as kalshi_markets
        on candidates.market_id = kalshi_markets.ticker
    left join prediction_markets.polymarket_markets as polymarket_markets
        on candidates.market_id = polymarket_markets.id
    left join prediction_markets.kalshi_series as kalshi_series
        on kalshi_markets.series_ticker = kalshi_series.ticker
    where not exists (
        select 1 from prediction_markets.alerts a
        where a.alert_id = candidates.alert_id
    )
)

select *
from alerts
;
