create or replace pipe MANAGE_DB.PIPES.ALBUM_PIPE auto_ingest=true 
as 
copy into SPOTIFY_DB.SPOTIFY_TOP_50.ALBUMS
from @MANAGE_DB.EXTERNAL_STAGES.SPOTIFY_S3_BUCKET
pattern = 'album_data/.*';

create or replace pipe MANAGE_DB.PIPES.ARTIST_PIPE auto_ingest=true 
as 
copy into SPOTIFY_DB.SPOTIFY_TOP_50.ARTISTS
from @MANAGE_DB.EXTERNAL_STAGES.SPOTIFY_S3_BUCKET
pattern = 'artist_data/.*';

create or replace pipe MANAGE_DB.PIPES.SONG_PIPE auto_ingest=true 
as 
copy into SPOTIFY_DB.SPOTIFY_TOP_50.SONGS
from @MANAGE_DB.EXTERNAL_STAGES.SPOTIFY_S3_BUCKET
pattern = 'song_data/.*';