package org.kodluyoruz

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.{Success, Try}


object App {

  def main(args: Array[String]): Unit = {

    val filePath = args(0)
    val outputPath = args(1)

    val sparkSession = SparkSession
      .builder()
      .appName("kodluyoruz-spark-project")
      .getOrCreate()

    val rawData = sparkSession.sparkContext.textFile(filePath)
/*
id,member_id,loan_amnt,funded_amnt,funded_amnt_inv,term,int_rate,installment,grade,sub_grade,emp_title,emp_length,home_ownership,annual_inc,verification_status,issue_d,loan_status,pymnt_plan,url,desc,purpose,title,zip_code,addr_state,dti,delinq_2yrs,earliest_cr_line,inq_last_6mths,mths_since_last_delinq,mths_since_last_record,open_acc,pub_rec,revol_bal,revol_util,total_acc,initial_list_status,out_prncp,out_prncp_inv,total_pymnt,total_pymnt_inv,total_rec_prncp,total_rec_int,total_rec_late_fee,recoveries,collection_recovery_fee,last_pymnt_d,last_pymnt_amnt,next_pymnt_d,last_credit_pull_d,collections_12_mths_ex_med,mths_since_last_major_derog,policy_code,application_type,annual_inc_joint,dti_joint,verification_status_joint,acc_now_delinq,tot_coll_amt,tot_cur_bal,open_acc_6m,open_il_6m,open_il_12m,open_il_24m,mths_since_rcnt_il,total_bal_il,il_util,open_rv_12m,open_rv_24m,max_bal_bc,all_util,total_rev_hi_lim,inq_fi,total_cu_tl,inq_last_12m
 */
    case class RawDataQ1(loan: Long, funded: Long)

    val mappedRawData = rawData.map{ line =>
      val fields = line.split(",")
      Try(RawDataQ1(fields(2).toLong, fields(3).toLong))
    }.collect{
      case Success(s) => s
    }

    val count = mappedRawData.filter(r => r.funded == r.loan).count()


    case class RawDataQ2(loanAmount: Long, income: Long, payment: Long)
    case class RawDataQ2Extended(range: String, loanAmount: Long, income: Long, payment: Long)

    val mappedRawDataQ2 = rawData.map{ line =>
      val fields = line.split(",")
      Try(RawDataQ2(fields(2).toLong, fields(13).toLong, fields(45).toLong))
    }.collect{
      case Success(value) => value
    }

    val t = mappedRawDataQ2.map{ r =>
      RawDataQ2Extended(incomeRange(r.income), r.loanAmount, r.income, r.payment)
    }

  import sparkSession.implicits._
    val dataSetQ2 = sparkSession.createDataFrame(t)


    val k = dataSetQ2
      .groupBy("range")
      .agg(avg("loanAmount"), avg("payment"))











  }

  def incomeRange(income: Long): String ={
    /*
     <40k, 40-60k, 60-80k, 80-100k
     */
    if(income < 40000) "<40k"
    else if(income < 60000) "40-60k"
    else if(income < 80000) "60-80k"
    else "80-100k"
  }

}
