```sql
表
--观众表
SELECT * from  hive_sql_test1.t_user
--电影表
SELECT * from  hive_sql_test1.t_movie
--影片表
SELECT * from  hive_sql_test1.t_rating

```

```sql
一、展示电影ID为2116这部电影各年龄段的平均影评分
1.根据影评表t_rating获取所有人(userid)对该电影的评分rate
SELECT r.userid, r.rate from  hive_sql_test1.t_rating r where movieid = 2116

2.影评表t_rating根据userid关联用户表拿到年龄字段
selet a.userid, a.age, b.rate from hive_sql_test1.t_user a inner join t_rating b on a.userid = b.userid where b.movieid = 2116

3.根据年龄age的求评分rate的平均值
select a.age, avg(b.rate) avgrate from hive_sql_test1.t_user a inner join hive_sql_test1.t_rating b on a.userid = b.userid where b.movieid = 2116 group by a.age

最终结果：
1.
with a as 
(select userid, rate from  hive_sql_test1.t_rating where movieid = 2116),
b as
(select userid, age  from hive_sql_test1.t_user) 
select b.age, avg(a.rate) avgrate from a inner join b on a.userid = b.userid group by b.age
Time taken: 49.457 seconds

2.
select a.age, avg(b.rate) avgrate from hive_sql_test1.t_user a inner join hive_sql_test1.t_rating b on a.userid = b.userid where b.movieid = 2116 group by a.age
Time taken: 47.957 seconds

比较，两个sql运行耗时差不多
```



```sql
二、找出男性评分最高且评分次数超过50次的10部电影，展示电影名，平均影评分和评分次数
思路：
找字段男性(sex),评分(rate),电影(moviename)
平均影评分avg(rate)
评分次数sum(userid)
1.找出电影名
select movieid, moviename from hive_sql_test1.t_movie
2.找出影评分，用户
select userid, movieid, rate from hive_sql_test1.t_rating
3.找出男性
select userid, sex from hive_sql_test1.t_user where sex = 'M'
4.找出男性，电影名的评分
5.求出平均影评分最高的10个，且评价次数大于50次
with a as
--找出电影名
(select movieid, moviename from hive_sql_test1.t_movie),
--找出影评分，用户
b as
(select userid, movieid, rate from hive_sql_test1.t_rating),
--找出男性
c as
(select userid, sex from hive_sql_test1.t_user where sex = 'M')
--求出平均影评分最高的10个，且评价次数大于50次
select sex, moviename, avg(rate) avgrate,count(1) total from b inner join a on b.movieid = a.movieid
inner join c on b.userid = c.userid group by moviename,sex having count(1) >=50 order by avgrate desc limit 10
```



```sql
三、找出影评次数最多的女士所给出最高分的10部电影的平均影评分，展示电影名和平均影评分（可使用多行SQL）
1.影评次数做多的女士(每部电影的次数最多女士不一样)
a.影评次数做多的
select userid, count(1) num from hive_sql_test1.t_rating group by userid order by num desc limit 1 
b.女士
select userid, sex from hive_sql_test1.t_user where sex = 'F'
找到该女士
select userid, count(1) num from hive_sql_test1.t_rating a inner join hive_sql_test1.t_user b on a.userid = b.userid and b.sex = 'F' group by userid order by num desc limit 1
2.评分最高的10部电影
a.电影名，评分
select a.moviename, b.rate from hive_sql_test1.t_movie a inner join hive_sql_test1.t_rating b on a.movieid = b.movieid
b.平均影评分
select moviename, avg(rate) from hive_sql_test1.t_rating a inner join hive_sql_test1.t_movie b on a.movieid = b.movieid group by moviename

-- 查询该女士评分最高的10部电影
with t1 as 
-- 找出评分最多的女士
(select r.userid, count(1) num from hive_sql_test1.t_rating r inner join hive_sql_test1.t_user u on r.userid = u.userid and u.sex = 'F' group by r.userid order by num desc limit 1),
t2 as
-- 找出该女生评分最高的10部电影
(select b.userid, b.movieid, a.moviename, b.rate from hive_sql_test1.t_movie a inner join hive_sql_test1.t_rating b on a.movieid = b.movieid inner join t1 on b.userid = t1.userid order by b.rate desc limit 10)
-- 找出电影名称及平均分
select t2.moviename, avg(t3.rate) avgrate from hive_sql_test1.t_rating t3 inner join t2 on t3.movieid = t2.movieid group by t2.moviename order by avgrate desc

```

