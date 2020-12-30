package structuredApi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr,col,column,max}

object Joins extends App{
  val spark = SparkSession.builder()
    .appName("'Join' The Party")
    .config("spark.master","local")
    .getOrCreate()

  val sc = spark.sparkContext

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName : String) = spark.read
    .format("jdbc")
    .option("driver",driver)
    .option("url",url)
    .option("user",user)
    .option("password",password)
    .option("dbtable",s"public.$tableName")
    .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  /*
  employeesDF.show()
  salariesDF.show()
  deptManagersDF.show()
  titlesDF.show()
   */

  // 1. show all employees and their max salary

  val maxSalariesDF = salariesDF.groupBy("emp_no").agg(max("salary").as("max_salary"))
  val employeesMaxSalary = employeesDF.join(maxSalariesDF,"emp_no")

  employeesMaxSalary.show()

  // 2. show all employees who were never managers
  val empNeverManagesDF = employeesDF.join(deptManagersDF,usingColumns=Seq{"emp_no"},joinType = "left_anti")
  empNeverManagesDF.show()

  // 3. find the most recent job titles of the best paid 10 employees
  val mostRecentJobTitle = titlesDF.groupBy("emp_no","title").agg(max("to_date"))
  val bestPaidEmployeesDF = employeesMaxSalary.orderBy(col("max_salary").desc).limit(10)
  val bestPaidJobTitleDF =  bestPaidEmployeesDF.join(mostRecentJobTitle,"emp_no")
  bestPaidJobTitleDF.show()

}
