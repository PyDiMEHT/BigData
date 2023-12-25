1 
Философ - Thread

Вилки - мьютексы

Количество мест - количество философов

В методе eat() создается эфимерный узел, затем при помощи метода aquire() филосов берет правую и левую вилки. Поток засыпает потом освобождается.В методе think() эфимерный узел удаляется и поток засыпает.

```python
Philosopher 2 takes right fork
Philosopher 1 takes right fork
Philosopher 1 puts right fork
Philosopher 0 takes right fork
Philosopher 1 takes left fork
Philosopher 4 takes right fork
Philosopher 3 takes right fork
Philosopher 3 puts right fork
Philosopher 3 takes left fork
Philosopher 2 puts right fork
Philosopher 2 takes left fork
Philosopher 0 puts right fork
Philosopher 4 puts right fork
Philosopher 0 takes left fork
Philosopher 4 takes left fork
Philosopher 0 puts left fork
Philosopher 0 is thinking
Philosopher 1 puts left fork
Philosopher 1 is thinking
Philosopher 3 puts left fork
Philosopher 3 is thinking
Philosopher 2 puts left fork
Philosopher 2 is thinking
...
```

2
Реализован вариант 2.

Координатор(Coordinator) регистрирует транзакционный узел /root + "/coordinator"

Первая фаза:

Координатор уведомляет Worker о транзакции

Координатор устанавливает WATCH

Каждый исполнитель создает эфемерных узел с решением commit/abort

Исполнитель подписывается на события своего узла для получения решения от координатора

Вторая фаза:

Координатор принимает решение (по большинству голосов) о commit/abort после ожидания таймаута или после создания всех узлов исполнителей с решением commit

Координатор изменяет значение эфемерных узлов для каждого исполнителя на commit / abort

Исполнители применяют / прерывают транзакцию

```python
register workers []
Worker 1 vote abort
Worker 4 vote abort
Worker 3 vote abort
Worker 2 vote commit
Worker 0 vote abort
All workers voted
abort
```
