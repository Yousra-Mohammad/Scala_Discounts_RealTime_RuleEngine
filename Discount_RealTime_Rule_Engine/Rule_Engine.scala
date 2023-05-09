/////////////////////////////////////import libraries///////////////////////////////////////////////////////////////////

// Import libraries:

import java.time.{LocalDate, LocalDateTime}
import java.time.temporal.ChronoUnit
import java.io.{BufferedReader, FileReader}
import java.nio.file.{Files, Path, Paths, StandardWatchEventKinds, WatchEvent, WatchService}
import java.sql.{Connection, DriverManager, Timestamp}
import java.nio.file.StandardWatchEventKinds._
import java.io._
import scala.::



object Rule_Engine extends App{

/////////////////////////////////////Handling Database Connection///////////////////////////////////////////////////////


  // Database information
  val url = "jdbc:mysql://localhost:3306/transactions"
  val username = "root"
  val password = "Dina123"
  val table = "discountstable"
  val logTable = "logstable"

  // Connect with database
  Class.forName("com.mysql.cj.jdbc.Driver")
  val conn: Connection = DriverManager.getConnection(url, username, password)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


///////////////////////////////Handling Reading Files from a Directory//////////////////////////////////////////////////


  // define 'using' block to ensure that a resource is properly closed after it has been used
  // this works even if an exception is thrown
  // it will be used with reading and processing functions to ensure closing files after using

  /**
   *
   *  Executes a function that uses a resource of type A, ensuring that the resource is closed afterwards.
   *  @param resource the resource to use in the function, must have a close() method
   *  @param f        the function to execute with the resource
   *  @tparam A the type of resource, which must have a close() method
   *  @tparam B the return type of the function f
   *  @return the result of the function f
   */

  def using[A <: {def close(): Unit}, B](resource: A)(f: A => B): B = {
    try {
      f(resource)
    } finally {
      resource.close()
    }
  }



  // Directory Path
  val dirPath: Path = Paths.get("row_data")

  // create backup file from the data into scv file
  // the backup files path
  val bkupDisounts = "output_data/DiscountTableBkup.csv"
  val bkupLogs = "output_data/LogsTableBkup.csv"


  // initialize WatchService
  val watchService: WatchService = dirPath.getFileSystem.newWatchService()

  // register the directory to watch for create events
  dirPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE)

  // run the watch service loop
  while (true) {

    // wait for the next event
    val key = watchService.take()

    // check if the event is for a CSV file
    if (key != null) {

      // process each event in the key
      key.pollEvents().forEach { event =>

        // check if the event is for a CSV file
        if (event.kind() == ENTRY_CREATE && event.context().toString.endsWith(".csv")) {

          // get the file path
          val dirPath: Path = key.watchable().asInstanceOf[Path]
          val filePath: Path = dirPath.resolve(event.context().asInstanceOf[Path])

          // wait for a short time to make sure the file is ready to be read
          // This line is for handling the "The process cannot access the file because it is being used by another process" error
          Thread.sleep(50)

          // read the CSV file
          // make sure to close the file after reading so it won't cause any errors
          // read the file into list of list of strings 'list[list[string]]'
          val reader = new BufferedReader(new FileReader(dirPath.resolve(event.context().toString).toFile))
          val lines = try {
            reader.readLine()
            Iterator.continually(reader.readLine()).takeWhile(_ != null).toList.map(_.split(",").toList)
          } finally {

            reader.close()
          }

          // store the read timestamp to br used in logs data
          val readTime = Timestamp.valueOf(LocalDateTime.now())
		  
		      // get the processed file name for logs data
		  
          val fileName = filePath.getFileName.toString

          println(s"Start Time $readTime")
          println(s"Listener Detected file: $fileName")
          println(s"$fileName Successfully Read")


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////Start Data Processing/////////////////////////////////////////////////////////////

          // put the processing into using block so the file will be close after processing is finished
          using(new BufferedReader(new FileReader(dirPath.resolve(event.context().toString).toFile))) { reader =>

            // apply all functions on the data list and store the result in a new list
            val result = lines.map(expiration_fn)
              .map(Product_category)
              .map(exact_day)
              .map(quantityDiscount)
              .map(appDiscount)
              .map(visaDiscount)
              .map(appliedDiscount)
              .map(finalPrice)

            // store the processing timestamp for logs data
            val processTime = Timestamp.valueOf(LocalDateTime.now())
            println(s"Producer Received file: $fileName")
            println(s"$fileName Successfully Processed")



            // create a set of the indexes of needed elements from the final list
            val indicesToKeep = Set(0, 1, 2, 3, 4, 5, 6, 13, 14)

            // filter the final data using the indexes of the element and store the result in a new list
            val finalData = result.map(innerList => innerList.zipWithIndex.filter { case (_, index) => indicesToKeep.contains(index) }.map(_._1))

            // filter the final data to have a new list of the transaction that have a discount
            val processedLinesList = finalData.filter(innerList => innerList(7) != 0)

            // get the length of the list with contains transactions with applied discounts for logs data
            val pprocessedLinesNum = processedLinesList.length

            // insert the final data list into the database
            finalData.foreach(insertIntoDatabase)

            // store the write timestamp for logs data
            val writeTimestamp = Timestamp.valueOf(LocalDateTime.now())

            println(s"File: $fileName Successfully wrote to the Database")
            println(s"Finish Time $writeTimestamp")

            

            

            // get the number of total transactions on the processed file for logs data
            val numRows = lines.length


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////Tracking the Logs Data////////////////////////////////////////////////////


            // Create Lists of different Logs Data to be stored on Database

            // create read logs data list
            val readlogs = List((readTime).toString, "Listener detected file " + fileName, numRows.toString + " Rows are Successfuly Read from the source")

            // create processing logs data list
            val processedlogs = List((processTime).toString, "The file " + fileName + " has total " + numRows.toString + " Transaction", pprocessedLinesNum.toString + " Transaction had a discount")

            // create write logs data list
            val writelogs = List((writeTimestamp).toString, "Producer recieved file " + fileName, numRows.toString + " Rows are Successfuly Wrote to the database")

            // write all logs data into the database
            writeToLogsTable(readlogs)
            writeToLogsTable(processedlogs)
            writeToLogsTable(writelogs)


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////Handling Backups Data////////////////////////////////////////////////////

            // write the discount data backup
            appendCSVFile(finalData,bkupDisounts)

            // create list of logs data lists
            val bkupLogsList = List(readlogs, processedlogs, writelogs)

            // write the logs data backup
            appendCSVFile(bkupLogsList,bkupLogs)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

            // close the file so it can be deleted from the source directory after processing
            reader.close()

          }


          // delete the processed CSV file from the source directory

          Files.delete(filePath)


        }
      }

      // reset the watcher key
      key.reset()
    }
  }


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


///////////////////////////////////////////////Handling Main Functions//////////////////////////////////////////////////



  // create a function to calculate the discount based on the expiration date
  // it returns list of any 'List[Any]' contains the actual data and the discount based on the expiration date

  /**
   *
   *  Calculates the discount to be applied to a product based on its expiration date.
   *  @param datalist a list of data, where the first element is the purchasing date,
   * the second element is the product name, the third element is the expiration date,
   * the forth element is the quantity, the fifth element is the payment channel,
   * and the sixth element is the payment method
   * @return a new list with the same data points as datalist, plus an additional element representing the discount to be applied
   * based on the expiration date
   */

  def expiration_fn(datalist: List[Any]): List[Any] = {

    val Purchasing_Date = LocalDate.parse(datalist(0).toString.split("T").toList.head)
    val exp_date = LocalDate.parse(datalist(2).toString)
    val Days_between = ChronoUnit.DAYS.between(Purchasing_Date, exp_date).toInt

    val Discount =
      if (Days_between < 30 && Days_between > 0) {
      (30 - Days_between)
    } else {
      0
    }

    val outList = datalist :+ Discount

    outList
  }


  // create a product category function to calculate the discount based on the product category
  // it returns list of any 'List[Any]' contains the actual data with the expiration discount and the discount based on the product category

  /**
   *
   *  Determines the discount to be applied to a product based on its category.
   *  @param dataList a list of data, where the first element is the purchasing date, the second element is the product name,
   *  the third element is the expiration date, the forth element is the quantity, the fifth element is the payment channel,
   *  the sixth element is the payment method, and the seventh element is the expiration date discount
   *  @return a new list with the same data points as dataList, plus an additional element representing the discount to be applied
   *  based on the product category
   */

  def Product_category(dataList: List[Any]): List[Any] = {

    val product = dataList(1).toString.split("-").toList.head.trim

    val Discount =
      product.toLowerCase() match {
      case "cheese" => 10
      case "wine" => 5
      case _ => 0
    }

    val outList = dataList :+ Discount

    outList
  }



  // create an exact day function to calculate the discount based on a specific purchase day of month
  // it returns list of any 'List[Any]' contains the actual data with the expiration discount and product category discount in addition to the discount based on specific day of month

  /**
   *
   *  Determines the discount to be applied to a purchase made on a specific day.
   *  @param dataList a list of data, that contains purchasing date, product name, expiration date,
   *  the quantity, the payment channel, the payment method, the expiration date discount, and the product category discount
   *  @return a new list with the same data points as dataList, plus an additional element representing the discount to be applied
   *  based on specific day of month
   */

  def exact_day(dataList: List[Any]): List[Any] = {

    val purch_date = LocalDate.parse(dataList(0).toString.split("T").toList.head)
    val month = purch_date.getMonth().getValue()
    val dayOfMonth = purch_date.getDayOfMonth().toInt
    val date = LocalDate.ofYearDay(0, month * 31 + dayOfMonth)

    val Discount =
      if (month == 3 && dayOfMonth == 23) {
      50
    } else {
      0
      }

    val outList = dataList :+ Discount

    outList
  }


  // create the quantity function to calculate the discount based on a quantity of items bought
  // it returns list of any 'List[Any]' contains the actual data with the expiration discount, product category discount, specific day of month discount, and quantity discount

  /**
   *
   *  Determines the discount to be applied to a purchase based on the quantity of items bought.
   *  @param dataList a list of data, that contains purchasing date, product name, expiration date,
   *  the quantity, the payment channel, the payment method, the expiration date discount,
   *  the product category discount, and the specific day discount
   *  @return a new list with the same data points as dataList, plus an additional element representing
   *  the discount to be applied based on the items quantity
   */

  def quantityDiscount(dataList: List[Any]): List[Any] = {

    val quan = dataList(3).toString.toInt

    val Discount =
      if (quan >= 6 && quan <= 9) {
      5
    } else if (quan >= 10 && quan <= 14) {
      7
    }
    else if (quan >= 15) {
      10
    }
    else {
      0
    }

    val outList = dataList :+ Discount
    outList

  }



  // create the app discount function to calculate the discount based on purchasing channel
  // it returns list of any 'List[Any]' contains the actual data with the expiration discount, product category discount, specific day of month discount, quantity discount and channel discount.

  /**
   *
   *  Determines the discount to be applied to a purchase based on the purchasing channel.
   *  @param dataList a list of data, that contains purchasing date, product name, expiration date,
   *  the quantity, the payment channel, the payment method, the expiration date discount,
   *  the product category discount, the specific day discount, and channel discount
   *  @return a new list with the same data points as dataList, plus an additional element representing the discount
   *  to be applied based on the purchasing channel
   */

  def appDiscount(dataList: List[Any]): List[Any] = {

    val Channel = dataList(5)

    val Discount =
      if (Channel.toString.toLowerCase() == "app") {
      ((dataList(3).toString.toInt / 5) * 5) + (if (dataList(3).toString.toInt % 5 > 0) 5 else 0)
      } else {
        0
      }

    val outList = dataList :+ Discount

    outList

  }



  // create the visa discount function to calculate the discount based on payment method
  // it returns list of any 'List[Any]' contains the actual data with the expiration discount, product category discount, specific day of month discount, quantity discount, channel discount, and payment method discount

  /**
   *
   *  Determines the discount to be applied to a purchase based on the payment method.
   *  @param dataList a list of data , that contains purchasing date, product name, expiration date,
   *  the quantity, the payment channel, the payment method, the expiration date discount,
   *  the product category discount, the specific day discount, and channel discount
   *  @return a new list with the same data points as dataList, plus an additional element representing the discount to be applied
   *  based on the payment method
   */

  def visaDiscount(dataList: List[Any]): List[Any] = {

    val payment = dataList(6)

    val Discount =
      if (payment.toString.toLowerCase() == "visa") {
        5
      } else {
        0
      }

    val outList = dataList :+ Discount

    outList
  }



  // create applied discount function that calculate the discount should br applied for each purchasing transaction
  // it returns list of any 'List[Any]' contains actual data and all discounts in addition to the final discount that should be applied.

  /**
   *
   *  Determines the discount to be applied to a purchase based on the highest two discount values.
   *  @param dataList a list of data, that contains the actual data, and all discounts.
   *  @return a new list with the same data points as dataList, plus an additional element representing the discount to be applied
   */

  def appliedDiscount(dataList: List[Any]): List[Any] = {

    val discountList = List(dataList(7).toString.toDouble, dataList(8).toString.toDouble, dataList(9).toString.toDouble, dataList(10).toString.toDouble, dataList(11).toString.toDouble, dataList(12).toString.toDouble)
    val sortedDiscount = discountList.sorted.reverse.take(2)
    val Discount = (((sortedDiscount(0) / 100) + (sortedDiscount(1) / 100)) / 2)

    val outList = dataList :+ Discount

    outList
  }



  // create final price function that calculate the final price after applying the discount
  // it returns list of any 'List[Any]' contains actual data, all discounts values, applied discount, and the final price after applied discount.

  /**
   *
   * Determines the final price after applying the discount.
   *
   * @param dataList a list of data, that contains the actual data, all discounts values, and the discount to be apply
   * @return a new list with the same data points as dataList, plus an additional element representing the fina price after
   * the discount has been be applied
   */

  def finalPrice(dataList: List[Any]): List[Any] = {

    val Discount = ((dataList(4).toString.toDouble)*(dataList(3).toString.toDouble)) - (((dataList(4).toString.toDouble)*(dataList(3).toString.toDouble)) * dataList(13).toString.toDouble)


    val outList = dataList :+ Discount
    outList
  }



////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


/////////////////////////////////////////////////Handling Database Functions////////////////////////////////////////////


  // function to insert the main data into discount table in the database

  /**
   *
   * Write the final required transactions data into the discount table in MySQL database.
   *
   * @param dataList a list of data, that contains the required data
   *
   */

  def insertIntoDatabase(dataList: List[Any]): Unit = {

    val statement = conn.prepareStatement(s"INSERT INTO $table VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")

    for (i <- 0 until dataList.length) {
      statement.setObject(i + 1, dataList(i))
    }

    statement.execute()
    statement.close()

  }


  // function to insert the logs data into logs table in the database

  /**
   *
   * Write the logs data data into the discount table in MySQL database.
   *
   * @param dataList a list of data, that contains different logs data
   *
   */

  def writeToLogsTable(dataList: List[String]): Unit = {

    val statement = conn.prepareStatement(s"INSERT INTO $logTable VALUES (?, ?, ?)")

    for (i <- 0 until dataList.length) {
      statement.setObject(i + 1, dataList(i))
    }
    statement.execute()
    statement.close()


  }


////////////////////////////////////////Handling Backup Files///////////////////////////////////////////////////////////


  // create the write CSV function
  // the function will append the data into the existing CVS backup files

  /**
   *
   * Appends a List of List of Any data to a CSV file at a specified file path.
   * It creates a new file if the file doesn't exists.
   * @param dataList A List of List of Any type, containing the data to be written to the CSV file
   * @param filePath A String representing the file path of the CSV file to be written to
   *
   */

  def appendCSVFile(data: List[List[Any]], filePath: String): Unit = {

    val file = new File(filePath)
    val bw = new BufferedWriter(new FileWriter(file, true))

    for (row <- data) {
      bw.write(row.mkString(","))
      bw.newLine()
    }

    bw.close()
  }





}
