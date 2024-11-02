-- total record check view creation
create or replace view total_record_check as(
with alb as (
select count(*) as total_albums
from albums
),
art as (
select count(*) as total_artists
from artists
),
song as (
select count(*) as total_songs
from songs
)
select
    alb.total_albums,
    art.total_artists,
    song.total_songs
from alb, art, song);

-- Top 5 artists with most songs in top 50 playlist
create or replace view top_5_artists as (
select a.artist_name, count(*) as "Total tracks in top 50" 
from songs s
join artists a
on a.artist_id = s.artist_id
group by a.artist_name
order by "Total tracks in top 50" desc
limit 5)
;

-- Avg popularity for songs added in last quarter vs older songs.
create or replace view time_popularity_analysis as (
with quarter_analysis as(
select 
s.popularity,
case when extract (month, added_date) >= extract (month, getdate()) - 3 then 'yes' else 'no' end as last_quarter
from songs s)

select last_quarter, avg(POPULARITY) as average_popularity 
from quarter_analysis
group by last_quarter);


-- top 5 and bot 5 artists by popularity, song duration & track appearances.
create or replace view top_bot_artists_analytics as (
    with cte1 as (
    select 
        a.artist_name,
        a.artist_id,
        round(avg(s.popularity), 2) as "Avg Popularity Score",
        round(avg(s.song_duration_ms/1000),2) as "Avg Song Duration/min",
        count(*)  as "Track Appearances in Top 50"
    from songs s
    join artists a on a.artist_id = s.artist_id
    group by a.artist_name, a.artist_id),

    cte2 as (
    select 
    artist_id, 
    sum(a.total_tracks) as "Total Tracks in Album" 
    from albums a
    join songs s on s.album_id = a.album_id
    group by artist_id),

    final_table as (
    select 
        cte1.artist_name,
        "Avg Popularity Score",
        "Avg Song Duration/min",
        "Track Appearances in Top 50",
        "Total Tracks in Album",
        rank() over(order by "Avg Popularity Score" desc) as ranking
    from cte1
    join cte2 
    on cte1.artist_id = cte2.artist_id)

    select 
        "ARTIST_NAME",
        "Avg Popularity Score",
        "Avg Song Duration/min",
        "Track Appearances in Top 50",
        "Total Tracks in Album"
    from final_table
    where ranking <=5 or ranking >= (select (max(ranking) - 5) as lowest_rank from final_table))
;

-- Top song based on popularity
create or replace view top_song as (
select song_name, popularity
from songs
where popularity = (select max(popularity) from songs))
;