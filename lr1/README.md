## 1. Найти велосипед с максимальным временем пробега.

```python
bike_mileage = bike_by_id.mapValues(lambda x: x.duration).reduceByKey(lambda x1, x2: x1 + x2)
bike_max_mileage = bike_mileage.top(1, key=lambda x: x[1])[0]

```
складываем пробег по всем id и выбираем самый большой

## 2. Найти наибольшее геодезическое расстояние между станциями.

```python
def calculate_distance(lat1, lon1, lat2, lon2):
    # Convert latitude and longitude to radians
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Calculate the differences between the latitudes and longitudes
    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    # Apply the Haversine formula to calculate the distance
    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    distance = 6371 * c  # Radius of the Earth in kilometers

    return distance

    stat = pairs.map(lambda pair: (pair[0].station_id, pair[1].station_id, calculate_distance(pair[0].lat,pair[0].long,pair[1].lat,pair[1].long)))
    max_diff_pair = stat.reduce(lambda pair1, pair2: pair1 if pair1[2] > pair2[2] else pair2)
```

находим расстояние от каждого к каждому по широте и долготе и выводим самое большое


## 3. Найти путь велосипеда с максимальным временем пробега через станции.

```python
bike_path = tripsObj.filter(lambda x: x.bike_id == bike_duration_top)\
.sortBy(lambda x: x.start_date).map(lambda x: (x.start_station_name, x.end_station_name))
```
сортируем и выводим

## 4. Найти количество велосипедов в системе.

count_bikes = tripsObj.map(lambda x: x.bike_id).distinct().count()

просто считаем

## 5. Найти пользователей потративших на поездки более 3 часов.

users = tripsObj.filter(lambda x: x.duration > (3 * 60 * 60))\
.map(lambda x: x.zip_code).distinct()

print(users.count())

просто фильтруем и считаем