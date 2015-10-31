import dsl
import random

mysqlh = dsl.mapping.handler('MysqlQueryHandler')

result = dsl.took.Handler.Result()
result = mysqlh._fetch(sql='select * from a order by a.a',result=result)
print(result.status)
print(result.content)

result = dsl.took.Handler.Result()
result = mysqlh._execute(sql='insert into a values (%d)' % random.uniform(1, 50),result=result)
print(result.status)
print(result.content)

result = dsl.took.Handler.Result()
result = mysqlh._fetch(sql='select * from a order by a.a',result=result)
print(result.status)
print(result.content)
