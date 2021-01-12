package sparkSQL

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkSQL extends App {

  val spark = SparkSession.builder()
    .appName("SQL, SQL and more SQL")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  /**
    * Exercises
    *
    * 1. Read the movies DF and store it as a Spark table in the rtjvm database.
    * 2. Count how many employees were hired in between Jan 1 1999 and Jan 1 2000.
    * 3. Show the average salaries for the employees hired in between those dates, grouped by department.
    * 4. Show the name of the best-paying department for employees hired in between those dates.
    */

  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")
  databasesDF.show()

  // transfer tables from a DB to Spark tables
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false):Unit = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)

    if (shouldWriteToWarehouse) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }

  transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager")
  )

  // read DF from loaded Spark tables
  val employeesDF2 = spark.read.table("employees")

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("movies")

  spark.sql(
    """
      |select count(*)
      |from employees
      |where hire_date > '1999-01-01' and hire_date < '2000-01-01'
    """.stripMargin
  ).show()

  spark.sql(
    """
      |select de.dept_no, avg(s.salary)
      |from employees e
      |inner join dept_emp de
      | on de.emp_no = e.emp_no
      |inner join salaries s
      |  on s.emp_no = e.emp_no
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |group by de.dept_no
    """.stripMargin
  ).show()

  spark.sql(
    """
      |select avg(s.salary) payments, d.dept_name
      |from employees e
      |inner join dept_emp de
      | on de.emp_no = e.emp_no
      |inner join salaries s
      | on s.emp_no = e.emp_no
      |inner join departments d
      | on d.dept_no = de.dept_no
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      |group by d.dept_name
      |order by payments desc
      |limit 1
    """.stripMargin
  ).show()
}
