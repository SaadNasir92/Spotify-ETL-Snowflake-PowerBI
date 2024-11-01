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

select * from total_record_check;

