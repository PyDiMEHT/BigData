читаем язычи и заключаем в скобки

```python
languages_df = languages_df.withColumn("name", concat(lit("<"), lower(col("name")), lit(">")))
languages_df.take(10)
[Row(name='<a# .net>', wikipedia_url='https://en.wikipedia.org/wiki/A_Sharp_(.NET)'),
 Row(name='<a# (axiom)>', wikipedia_url='https://en.wikipedia.org/wiki/A_Sharp_(Axiom)'),
 Row(name='<a-0 system>', wikipedia_url='https://en.wikipedia.org/wiki/A-0_System'),
 Row(name='<a+>', wikipedia_url='https://en.wikipedia.org/wiki/A%2B_(programming_language)'),
 Row(name='<a++>', wikipedia_url='https://en.wikipedia.org/wiki/A%2B%2B'),
 Row(name='<abap>', wikipedia_url='https://en.wikipedia.org/wiki/ABAP'),
 Row(name='<abc>', wikipedia_url='https://en.wikipedia.org/wiki/ABC_(programming_language)'),
 Row(name='<abc algol>', wikipedia_url='https://en.wikipedia.org/wiki/ABC_ALGOL'),
 Row(name='<abset>', wikipedia_url='https://en.wikipedia.org/wiki/ABSET'),
 Row(name='<absys>', wikipedia_url='https://en.wikipedia.org/wiki/ABSYS')]
```

читаем 
```python
posts_schema = StructType([
    StructField("_Id", StringType(), True),
    StructField("_CreationDate", StringType(), True),
    StructField("_Tags", StringType(), True)
])

posts_df = sc.read.format("xml") \
    .option("rowTag", "row") \
    .schema(posts_schema) \
    .load("posts_sample.xml")

# Разбивка Tags на языки программирования
posts_with_languages_df = posts_df.join(languages_df, col("_Tags").contains(col("name")))

posts_with_languages_df.take(10)


[Row(_Id='3778222', _CreationDate='2010-09-23T12:13:59.443', _Tags='<java>', name='<java>', wikipedia_url='https://en.wikipedia.org/wiki/Java_(programming_language)'),
 Row(_Id='3798872', _CreationDate='2010-09-26T17:07:04.840', _Tags='<php><wordpress><memory>', name='<php>', wikipedia_url='https://en.wikipedia.org/wiki/PHP'),
 Row(_Id='3833655', _CreationDate='2010-09-30T18:27:56.320', _Tags='<ruby><rvm>', name='<ruby>', wikipedia_url='https://en.wikipedia.org/wiki/Ruby_(programming_language)'),
 Row(_Id='3838866', _CreationDate='2010-10-01T11:52:42.210', _Tags='<c><sizeof>', name='<c>', wikipedia_url='https://en.wikipedia.org/wiki/C_(programming_language)'),
 Row(_Id='3859099', _CreationDate='2010-10-04T21:05:50.150', _Tags='<php><sql>', name='<php>', wikipedia_url='https://en.wikipedia.org/wiki/PHP'),
 Row(_Id='3872977', _CreationDate='2010-10-06T13:31:25.900', _Tags='<python><sungridengine><qsub>', name='<python>', wikipedia_url='https://en.wikipedia.org/wiki/Python_(programming_language)'),
 Row(_Id='3885802', _CreationDate='2010-10-07T20:53:59.650', _Tags='<javascript><jquery><wysiwyg>', name='<javascript>', wikipedia_url='https://en.wikipedia.org/wiki/JavaScript'),
 Row(_Id='3886770', _CreationDate='2010-10-07T23:56:30.330', _Tags='<applescript><scrape>', name='<applescript>', wikipedia_url='https://en.wikipedia.org/wiki/AppleScript'),
 Row(_Id='3891676', _CreationDate='2010-10-08T14:44:33.467', _Tags='<php><timer>', name='<php>', wikipedia_url='https://en.wikipedia.org/wiki/PHP'),
 Row(_Id='3904423', _CreationDate='2010-10-11T07:54:55.923', _Tags='<php><embed>', name='<php>', wikipedia_url='https://en.wikipedia.org/wiki/PHP')]
```

преобразуем год

```python
from pyspark.sql.functions import to_date, year
posts_with_languages_df = posts_with_languages_df.withColumn("Year", year(to_date("_CreationDate", "yyyy-MM-dd'T'HH:mm:ss.SSS")))
posts_with_languages_df.take(10)

[Row(_Id='3778222', _CreationDate='2010-09-23T12:13:59.443', _Tags='<java>', name='<java>', wikipedia_url='https://en.wikipedia.org/wiki/Java_(programming_language)', Year=2010),
 Row(_Id='3798872', _CreationDate='2010-09-26T17:07:04.840', _Tags='<php><wordpress><memory>', name='<php>', wikipedia_url='https://en.wikipedia.org/wiki/PHP', Year=2010),
 Row(_Id='3833655', _CreationDate='2010-09-30T18:27:56.320', _Tags='<ruby><rvm>', name='<ruby>', wikipedia_url='https://en.wikipedia.org/wiki/Ruby_(programming_language)', Year=2010),
 Row(_Id='3838866', _CreationDate='2010-10-01T11:52:42.210', _Tags='<c><sizeof>', name='<c>', wikipedia_url='https://en.wikipedia.org/wiki/C_(programming_language)', Year=2010),
 Row(_Id='3859099', _CreationDate='2010-10-04T21:05:50.150', _Tags='<php><sql>', name='<php>', wikipedia_url='https://en.wikipedia.org/wiki/PHP', Year=2010),
 Row(_Id='3872977', _CreationDate='2010-10-06T13:31:25.900', _Tags='<python><sungridengine><qsub>', name='<python>', wikipedia_url='https://en.wikipedia.org/wiki/Python_(programming_language)', Year=2010),
 Row(_Id='3885802', _CreationDate='2010-10-07T20:53:59.650', _Tags='<javascript><jquery><wysiwyg>', name='<javascript>', wikipedia_url='https://en.wikipedia.org/wiki/JavaScript', Year=2010),
 Row(_Id='3886770', _CreationDate='2010-10-07T23:56:30.330', _Tags='<applescript><scrape>', name='<applescript>', wikipedia_url='https://en.wikipedia.org/wiki/AppleScript', Year=2010),
 Row(_Id='3891676', _CreationDate='2010-10-08T14:44:33.467', _Tags='<php><timer>', name='<php>', wikipedia_url='https://en.wikipedia.org/wiki/PHP', Year=2010),
 Row(_Id='3904423', _CreationDate='2010-10-11T07:54:55.923', _Tags='<php><embed>', name='<php>', wikipedia_url='https://en.wikipedia.org/wiki/PHP', Year=2010)]

```

группируем фильтруем и упорядочиваем 

```python
# Группировка по годам и языкам
grouped_df = posts_with_languages_df.groupBy("Year", "name").count()

window_spec = Window.partitionBy("Year").orderBy(col("count").desc())

ranked_df = grouped_df.withColumn("rank", F.rank().over(window_spec))

top10_df = ranked_df.filter(col("rank") <= 10).orderBy(col("Year").desc(), col("rank")).filter(col("Year") >= 2010)
top10_df.take(20)

[Row(Year=2019, name='<python>', count=166, rank=1),
 Row(Year=2019, name='<javascript>', count=135, rank=2),
 Row(Year=2019, name='<java>', count=95, rank=3),
 Row(Year=2019, name='<php>', count=65, rank=4),
 Row(Year=2019, name='<r>', count=37, rank=5),
 Row(Year=2019, name='<typescript>', count=17, rank=6),
 Row(Year=2019, name='<c>', count=14, rank=7),
 Row(Year=2019, name='<bash>', count=11, rank=8),
 Row(Year=2019, name='<matlab>', count=9, rank=9),
 Row(Year=2019, name='<dart>', count=9, rank=9),
 Row(Year=2019, name='<go>', count=9, rank=9),
 Row(Year=2018, name='<python>', count=220, rank=1),
 Row(Year=2018, name='<javascript>', count=198, rank=2),
 Row(Year=2018, name='<java>', count=146, rank=3),
 Row(Year=2018, name='<php>', count=111, rank=4),
 Row(Year=2018, name='<r>', count=66, rank=5),
 Row(Year=2018, name='<typescript>', count=27, rank=6),
 Row(Year=2018, name='<c>', count=24, rank=7),
 Row(Year=2018, name='<scala>', count=23, rank=8),
 Row(Year=2018, name='<powershell>', count=13, rank=9)]
```


выводим и сохраняем 

```python
tables_dict = {}
for year in top10_df.select("Year").distinct().rdd.map(lambda x: x.Year).collect():
    tables_dict[year] = top10_df.filter(col("Year") == year)

# Вывод примера для первых 2 годов
for year, table in tables_dict.items():
    print(f"Year: {year}")
    table.show()

Year: 2018
+----+------------+-----+----+
|Year|        name|count|rank|
+----+------------+-----+----+
|2018|    <python>|  220|   1|
|2018|<javascript>|  198|   2|
|2018|      <java>|  146|   3|
|2018|       <php>|  111|   4|
|2018|         <r>|   66|   5|
|2018|<typescript>|   27|   6|
|2018|         <c>|   24|   7|
|2018|     <scala>|   23|   8|
|2018|<powershell>|   13|   9|
|2018|      <bash>|   12|  10|
+----+------------+-----+----+

Year: 2015
+----+-------------+-----+----+
|Year|         name|count|rank|
+----+-------------+-----+----+
|2015| <javascript>|  277|   1|
|2015|       <java>|  209|   2|
|2015|        <php>|  167|   3|
|2015|     <python>|  121|   4|
|2015|          <r>|   43|   5|
...
|2017| <powershell>|   14|  10|
|2017|       <bash>|   14|  10|
+----+-------------+-----+----+

# Сохранение отчета в формате Parquet
top10_df.write.parquet("report.parquet", mode="overwrite")
for year, table in tables_dict.items():
    table.write.format("parquet").save(f"report_dict_{year}")
```