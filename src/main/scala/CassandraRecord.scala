import com.datastax.driver.core.{ConsistencyLevel, Session}

import scala.collection.JavaConverters._

  object CassandraRecord extends App with CassandraProvider {
    cassandraSession.getCluster.getConfiguration.getQueryOptions.setConsistencyLevel(ConsistencyLevel.QUORUM)

    createTable(cassandraSession)
  insertRecord(cassandraSession)
getRecordById(cassandraSession)
updateRecord(cassandraSession)
getRecordBySalary(cassandraSession)
getRecordByCity(cassandraSession)
deleteDataByCity(cassandraSession)

    if(!cassandraSession.isClosed){
      cassandraSession.close()
    }

    private def createTable(cassandraSession: Session): Unit = {
      cassandraSession.execute(s"create table IF NOT EXISTS record(emp_id int, emp_name text, emp_city text,emp_salary varint, emp_phone varint, PRIMARY KEY(emp_id,emp_salary))")
      cassandraSession.execute(s"create table IF NOT EXISTS record_city(emp_id int, emp_name text, emp_city text,emp_salary varint, emp_phone varint, PRIMARY KEY(emp_city))")
      cassandraSession.execute("create index if not exists city_index  on record(emp_city)")
    }

    private def insertRecord(cassandraSession: Session): Unit = {

      cassandraSession.execute("insert into record(emp_id,emp_name,emp_city,emp_salary,emp_phone) values(1,'neel','delhi',21000,9876543210)")
      cassandraSession.execute("insert into record(emp_id,emp_name,emp_city,emp_salary,emp_phone) values(2,'ankit','kerala',50000,1234567890)")
      cassandraSession.execute("insert into record(emp_id,emp_name,emp_city,emp_salary,emp_phone) values(3,'saurav','chandigarh',11000,2389945899)")
      cassandraSession.execute("insert into record(emp_id,emp_name,emp_city,emp_salary,emp_phone) values(2,'nishu','delhi',24000,1234563366)")

      cassandraSession.execute("insert into record_city(emp_id,emp_name,emp_city,emp_salary,emp_phone) values(1,'neel','delhi',21000,9876543210)")
      cassandraSession.execute("insert into record_city(emp_id,emp_name,emp_city,emp_salary,emp_phone) values(2,'ankit','kerala',50000,1234567890)")
      cassandraSession.execute("insert into record_city(emp_id,emp_name,emp_city,emp_salary,emp_phone) values(3,'saurav','chandigarh',11000,2389945899)")
      cassandraSession.execute("insert into record_city(emp_id,emp_name,emp_city,emp_salary,emp_phone) values(2,'nishu','delhi',24000,1234563366)")
    }

    private def getAllRecord(cassandraSession: Session): Unit = {
      val data = cassandraSession.execute("select * from record").asScala.toList
      println("--------------fetching all records from record table----------------")
      data.foreach(println(_))
    }

    private def getRecordById(cassandraSession: Session): Unit = {

      val data = cassandraSession.execute("select * from record where emp_id= 3").asScala.toList
      println("-------------fetch record where emp_id=3 from record table----------------")

      data.foreach(println(_))
      println()
    }

    private def updateRecord(cassandraSession: Session): Unit = {
      cassandraSession.execute(s"update record set emp_city='chandigarh' where emp_id=1 AND emp_salary=21000")
      getAllRecord(cassandraSession)
      println()
    }

    private def getRecordBySalary(cassandraSession: Session): Unit = {
      val data = cassandraSession.execute(s"select * from record where emp_id =2 AND emp_salary >20000 ")
      println("-----------fetch record salary where greater than 20000------------")
      data.forEach(println(_))
      println()
    }

    private def getRecordByCity(cassandraSession: Session): Unit = {
      val data = cassandraSession.execute(s"select * from record where emp_city ='chandigarh'").asScala.toList
      println("-----------fetch data from record table  where city='chandigarh'------------")

      data.foreach(println(_))
      println()
    }

    private def deleteDataByCity(cassandraSession: Session): Unit = {

      cassandraSession.execute(s"delete from record_city where emp_city = 'chandigarh'")
      val data = cassandraSession.execute("select * from record_city").asScala.toList
      println("-----------deleting record of employees from second table record_city where city='chandigarh'------------")

      data.foreach(println(_))
    }
  }
