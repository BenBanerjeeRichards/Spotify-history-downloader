select count(*)
from play;

select *
from play
order by played_at
desc
limit 10;

select
  track_name,
  main_artist_name,
  count(track_id)
from play
group by track_id
order by count(track_id)
desc;

select
  main_artist_name,
  count(main_artist_name)
from play
group by main_artist_id
order by count(main_artist_name)
desc;


select "spotify:track:" || track_id
from play
group by track_id
order by count(track_id)
desc
limit 500;

-- Summer throwback
select "spotify:track:" || track_id, count(track_id), play.* from play
where played_at >= "2018-05" and played_at <= "2018-09"
group by track_id
having count(track_id) > 10
order by count(track_id) desc
