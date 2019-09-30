package model

import conf.AppConf
//import model.Process.{capRejLkpGrpInd, writeContact, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

case class SrcFiles(tocorganization: DataFrame, tocpartycontractrole: DataFrame, toccontractstub: DataFrame,
                    todomaininstance: DataFrame, tocpointofcontact: DataFrame, lookupgroupind: DataFrame, mstr_org_tbl: DataFrame)
object Process {

  def apply(appConf: AppConf, spark: SparkSession) = {

    val readInputFilesList = Reader.textReader(spark, appConf.inputFilesPath).collect().map(_.toString)
    val inputFiles = readInputFilesList.map(inputPath => Reader.csvReader(spark, appConf.inputBasePath + "/" + inputPath))
    val buildInput = buildInputDataSets(inputFiles)
    val resjoin6befFilter = join6befFilter(joinTocOrgWithToc(buildInput.tocorganization, buildInput.tocpartycontractrole,
      buildInput.toccontractstub, buildInput.todomaininstance), selGrpCntc(buildInput.tocpointofcontact))
    val resselCol = selCol(resjoin6befFilter)
    val reslkpGrpInd = lkpGrpInd(buildInput.lookupgroupind, buildInput.mstr_org_tbl)
    val resjoinLkpGrpInd = joinLkpGrpInd(resselCol, reslkpGrpInd)
    val resfilterLkpGrpIndnulls = filterLkpGrpIndnulls(resjoinLkpGrpInd)
    val resrejLkpGrpInd = rejLkpGrpInd(resjoinLkpGrpInd)
    val rescapRejLkpGrpInd = capRejLkpGrpInd(resrejLkpGrpInd, reslkpGrpInd)
    val resfunnelMstrPrty = funnelMstrPrty(resfilterLkpGrpIndnulls,rescapRejLkpGrpInd)
    val reswriteContact = writeContact(spark, resfunnelMstrPrty)
    val reswriteToOutput =  writeToOutput(reswriteContact, appConf.outputFilesPath + "/" + appConf.ouputFileName)

  }
    def buildInputDataSets(readSrcFiles: Array[DataFrame]): SrcFiles = {

    SrcFiles(readSrcFiles(0).withColumn("toc_org_CABC", col("CABC")).withColumn("toc_org_IABC", col("IABC")).withColumn("toc_org_NameABC", col("NAMEABC")).withColumn("toc_org_CORPORATETAXNABC", col("CORPORATETAXNABC")).withColumn("toc_org_LASTUPDATEDATEABC", col("LASTUPDATEDATEABC")).drop("CABC", "IABC", "NAMEABC", "CORPORATETAXNABC", "STARTDATEABC", "LASTUPDATEDATEABC")
        .select(col("toc_org_CABC"),col("toc_org_IABC"),col("toc_org_NameABC"),col("toc_org_CORPORATETAXNABC"),col("toc_org_LASTUPDATEDATEABC"),col("PARTYTYPEABC")),
      readSrcFiles(1).select(col("C_OCPRTY_CONTRACTROLESABC"),col("I_OCPRTY_ContractRolesABC"),col("ROLEABC"),col("C_OCCTRSTB_CONTRACTROLESABC"),col("I_OCCTRSTB_CONTRACTROLESABC")),
      readSrcFiles(2).withColumn("toc_con_stb_ENDDATEABC", col("ENDDATEABC")).withColumn("toc_con_stb_STARTDATEABC", col("STARTDATEABC")).withColumn("toc_con_stb_CABC", col("CABC")).withColumn("toc_con_stb_IABC", col("IABC")).drop("ENDDATEABC", "STARTDATEABC", "CABC", "IABC")
          .select(col("toc_con_stb_ENDDATEABC"),col("toc_con_stb_STARTDATEABC"),col("toc_con_stb_CABC"),col("toc_con_stb_IABC"), col("CONTRACTSTATUABC"),col("ReferenceNumbABC")),
      readSrcFiles(3).select(col("IABC"), col("NAMEABC")),
      readSrcFiles(4),
      readSrcFiles(5),
      readSrcFiles(6))



  }

  def joinTocOrgWithToc(df_tocorganization: DataFrame, df_tocpartycontractrole: DataFrame, df_toccontractstub: DataFrame,
                        df_todomaininstance: DataFrame): DataFrame = {
    val finalDf = df_tocorganization
      .join(df_tocpartycontractrole, df_tocorganization("toc_org_CABC") === df_tocpartycontractrole("C_OCPRTY_CONTRACTROLESABC") && df_tocorganization("toc_org_IABC") === df_tocpartycontractrole("I_OCPRTY_CONTRACTROLESABC"), "left")
      .join(df_toccontractstub, df_tocpartycontractrole("C_OCCTRSTB_CONTRACTROLESABC") === df_toccontractstub("toc_con_stb_CABC") && df_tocpartycontractrole("I_OCCTRSTB_CONTRACTROLESABC") === df_toccontractstub("toc_con_stb_IABC"), "left")
      .join(df_todomaininstance, col("CONTRACTSTATUABC") === df_todomaininstance("IABC"), "left").withColumnRenamed("NAMEABC", "GRP_CNTRCT_STATUSABC").drop("IABC")
      .join(df_todomaininstance, col("PARTYTYPEABC") === df_todomaininstance("IABC"), "left").withColumnRenamed("NAMEABC", "PRTY_TYPABC").drop("IABC")
      .join(df_todomaininstance, col("ROLEABC") === col("IABC"), "left").withColumnRenamed("NAMEABC", "PRTY_ROLEABC")
    finalDf
  }

  def selGrpCntc(df_tocpointofcontact: DataFrame): DataFrame = {
    val selGrpCntc = df_tocpointofcontact
      .withColumn("C_OCPRTY_POINTSOFCONTAABC", when(col("C_OCPRTY_POINTSOFCONTAABC") === null, 0).otherwise(col("C_OCPRTY_POINTSOFCONTAABC")))
      .withColumn("EFFECTIVEFROMABC", when(col("EFFECTIVEFROMABC") === "NULL", "1800-01-01 00:00:00.000").otherwise("EFFECTIVEFROMABC"))
      .withColumn("EFFECTIVETOABC", when(col("EFFECTIVETOABC") === "NULL", "2100-01-01 00:00:00.000").otherwise("EFFECTIVETOABC"))
      .filter(col("C_OCPRTY_POINTSOFCONTAABC") > "0"
        && current_timestamp().between(col("EFFECTIVEFROMABC"), col("EFFECTIVETOABC"))
      )
      .select(
        col("C_OCPRTY_POINTSOFCONTAABC"), col("I_OCPRTY_POINTSOFCONTAABC"), col("CONTACTNAMEABC")
      )
      .groupBy("C_OCPRTY_POINTSOFCONTAABC", "I_OCPRTY_POINTSOFCONTAABC").agg(max("CONTACTNAMEABC").alias("GRP_CNTCABC")).cache()
    selGrpCntc
  }

  def join6befFilter(finalDf: DataFrame, selGrpCntc: DataFrame): DataFrame = {

    val join6befFilter = finalDf.join(selGrpCntc, finalDf("toc_org_CABC") === selGrpCntc("C_OCPRTY_POINTSOFCONTAABC") && finalDf("toc_org_IABC") === selGrpCntc("I_OCPRTY_POINTSOFCONTAABC"), "left")
      .select(finalDf("toc_org_CABC"), finalDf("toc_org_IABC"), finalDf("REFERENCENUMBABC"), finalDf("toc_org_NameABC").alias("ACCT_NMABC"), finalDf("toc_org_CORPORATETAXNABC").alias("GRP_TAX_IDABC")
        , finalDf("toc_con_stb_STARTDATEABC").alias("GRP_START_DTABC")
        , finalDf("toc_con_stb_ENDDATEABC").alias("GRP_TERM_DTABC"), finalDf("GRP_CNTRCT_STATUSABC"), finalDf("PRTY_TYPABC"), finalDf("PRTY_ROLEABC"), finalDf("toc_org_LASTUPDATEDATEABC")
        , selGrpCntc("GRP_CNTCABC"))
    val join6 = join6befFilter.filter(col("REFERENCENUMBABC").isNotNull)
    println("join6 data")
    join6
  }

  def selCol(join6: DataFrame): DataFrame = {
    val convert = udf((x: String) => Try {
      if (x.indexOfSlice(":") == -1)
        x.substring(0, x.length) else x.substring(0, x.indexOfSlice(":") - 0)
      //charIndex
    } match {
      case Success(value) => value
      case Failure(exception) =>
        println("convertCharIndex:" + x)
        throw exception
    }
    )


    val cnvCharIndex = udf((x: String) => Try {
      if (x.indexOfSlice(":") == -1)
        "" else x.substring(x.indexOfSlice(":") + 1, x.length)
    } match {
      case Success(value) => value
      case Failure(exception) =>
        println("cnvCharIndex:" + x)
        throw exception
    }
    )
    val selCol = join6.select(col("toc_org_CABC").alias("ORG_CABC"), col("toc_org_IABC").alias("ORG_IABC"),
      when(col("toc_org_IABC").isNull, "")
        .when(col("toc_org_IABC") > 0, concat_ws("~", col("toc_org_CABC"), col("toc_org_IABC"))).alias("PRTY_KEYABC")
      , convert(col("REFERENCENUMBABC")).alias("PRTY_GRP_NBRABC"),
      cnvCharIndex(col("REFERENCENUMBABC")).alias("PRTY_ACCT_NBRABC"),

      //,lit("vghbknj").alias("PRTY_GRP_NBRABC"),
      //lit("ABCDEF").alias("PRTY_ACCT_NBRABC"),
      col("ACCT_NMABC").alias("PRTY_ACCT_NMABC"), col("GRP_TAX_IDABC").alias("PRTY_TAX_IDABC")
      , col("GRP_START_DTABC").alias("PRTY_EFF_DTTMABC")
      , col("GRP_TERM_DTABC").alias("PRTY_END_DTTMABC"), col("GRP_CNTRCT_STATUSABC").alias("PRTY_STUTSABC")
      , col("PRTY_TYPABC"), col("PRTY_ROLEABC"),
      col("toc_org_LASTUPDATEDATEABC")
      , col("GRP_CNTCABC").alias(("PRTY_CONTC_NMABC"))
      , lit(null).alias("PRTY_CONT_NMABC")
      , lit(null).alias("PRTY_ERISA_INRABC")
    )
    selCol
  }

  def lkpGrpInd(df_lookupgroupind: DataFrame, df_mstr_org_tbl: DataFrame): DataFrame = {
    val lkpGrpInd = df_lookupgroupind.join(df_mstr_org_tbl, trim(df_lookupgroupind("CURR_CDABC")) === trim(df_mstr_org_tbl("ORG_NMABC")) && df_mstr_org_tbl("ORG_STUSABC") === "ACTIVE", "left")
      .select(trim(col("GROUP_NOABC")).cast("string").alias("GROUP_NOABC"), trim(col("ACCT_NOABC")).cast("string").alias("ACCT_NOABC")
        , col("GROUP_NMABC"), col("CURR_CDABC"), col("MKT_SEGMNTABC"), col("SITUS_STABC"),
        col("BOCABC"), col("ACCT_MGRABC"), col("ACCT_EXEC_NMABC"), col("SALES_CDABC"),
        col("RATING_STABC"), col("BILL_STABC"), col("NO_OF_LVSABC"), col("SRCABC"),
        col("ORG_SEQ_IDABC"), col("PRTY_FED_IDABC"), col("PRTY_SIC_CDABC"), col("PRTY_SIC_CD_DESCABC"),
        col("PRTY_SLS_CDABC"), col("PRTY_SLS_REP_NMABC")
      )
    lkpGrpInd
  }

  def joinLkpGrpInd(selCol: DataFrame, lkpGrpInd: DataFrame): DataFrame = {
    val joinLkpGrpInd = selCol.join(lkpGrpInd, selCol("PRTY_GRP_NBRABC") === lkpGrpInd("GROUP_NOABC") && selCol("PRTY_ACCT_NBRABC") === lkpGrpInd("ACCT_NOABC"), "left")
      .select(col("PRTY_KEYABC")
        , col("PRTY_GRP_NBRABC")
        , col("GROUP_NMABC").alias("PRTY_GRP_NMABC")
        , col("PRTY_ACCT_NBRABC"),
        col("PRTY_ACCT_NMABC")
        , col("PRTY_TYPABC"), col("PRTY_ROLEABC")
        , col("PRTY_STUTSABC")
        , col("PRTY_TAX_IDABC")
        , col("PRTY_EFF_DTTMABC")
        , col("PRTY_END_DTTMABC")
        , col("PRTY_CONTC_NMABC")
        , col("MKT_SEGMNTABC").alias("PRTY_MRK_SEGT_TYPEABC")
        , col("BOCABC").alias("PRTY_BENF_OFFICEABC")
        , col("BILL_STABC").alias("PRTY_BILL_STABC")
        , col("RATING_STABC").alias("PRTY_RATE_STABC")
        , lit(null).alias("PRTY_CONT_NMABC")
        , lit(null).alias("PRTY_ERISA_INRABC")
      )
    joinLkpGrpInd
  }

  def filterLkpGrpIndnulls(joinLkpGrpInd: DataFrame): DataFrame = {
    val filterLkpGrpIndnulls = joinLkpGrpInd.filter(col("PRTY_GRP_NMABC").isNotNull)
    filterLkpGrpIndnulls
  }

  def rejLkpGrpInd(joinLkpGrpInd: DataFrame): DataFrame = {

  val rejLkpGrpInd = joinLkpGrpInd.select(col("PRTY_KEYABC"), col("PRTY_GRP_NBRABC"), col("PRTY_GRP_NMABC"), col("PRTY_ACCT_NBRABC"),
    col("PRTY_ACCT_NMABC"), col("PRTY_TYPABC"), col("PRTY_ROLEABC"), col("PRTY_STUTSABC"), col("PRTY_TAX_IDABC")
    , col("PRTY_EFF_DTTMABC"), col("PRTY_END_DTTMABC"), col("PRTY_CONTC_NMABC"), col("PRTY_MRK_SEGT_TYPEABC")
    , col("PRTY_BENF_OFFICEABC"), col("PRTY_BILL_STABC"), col("PRTY_RATE_STABC"), col("PRTY_CONT_NMABC")
    , col("PRTY_ERISA_INRABC")
  ).filter(col("PRTY_GRP_NMABC").isNull)
  rejLkpGrpInd
}
  def capRejLkpGrpInd(rejLkpGrpInd: DataFrame, lkpGrpInd: DataFrame): DataFrame = {

    val capRejLkpGrpInd = rejLkpGrpInd.join(lkpGrpInd, rejLkpGrpInd("PRTY_GRP_NBRABC") === lkpGrpInd("GROUP_NOABC"), "Inner")
      .select(col("PRTY_KEYABC")
        , col("PRTY_GRP_NBRABC")
        , col("PRTY_GRP_NMABC")
        , col("PRTY_ACCT_NBRABC"),
        col("PRTY_ACCT_NMABC")
        , col("PRTY_TYPABC"), col("PRTY_ROLEABC")
        , col("PRTY_STUTSABC")
        , col("PRTY_TAX_IDABC")
        , col("PRTY_EFF_DTTMABC")
        , col("PRTY_END_DTTMABC")
        , col("PRTY_CONTC_NMABC")
        , col("PRTY_MRK_SEGT_TYPEABC")
        , col("PRTY_BENF_OFFICEABC")
        , col("PRTY_BILL_STABC")
        , col("PRTY_RATE_STABC")
        , col("PRTY_CONT_NMABC")
        , col("PRTY_ERISA_INRABC"))
    capRejLkpGrpInd
  }

  def funnelMstrPrty(filterLkpGrpIndnulls: DataFrame, capRejLkpGrpInd: DataFrame): DataFrame = {
    val funnelMstrPrty = filterLkpGrpIndnulls.union(capRejLkpGrpInd)

    funnelMstrPrty
  }

  def writeContact(sparkSession: SparkSession, funnelMstrPrty: DataFrame): DataFrame = {
    import sparkSession.implicits._
    val writeContact = funnelMstrPrty.select(concat_ws("|", $"PRTY_KEYABC", $"PRTY_GRP_NBRABC", $"PRTY_GRP_NMABC", $"PRTY_ACCT_NBRABC", $"PRTY_ACCT_NMABC", $"PRTY_TYPABC", $"PRTY_ROLEABC", $"PRTY_STUTSABC", $"PRTY_TAX_IDABC", $"PRTY_EFF_DTTMABC", $"PRTY_END_DTTMABC", $"PRTY_CONTC_NMABC", $"PRTY_MRK_SEGT_TYPEABC", $"PRTY_BENF_OFFICEABC", $"PRTY_BILL_STABC", $"PRTY_RATE_STABC", $"PRTY_CONT_NMABC", $"PRTY_ERISA_INRABC"))
    //val writeContact = finalDf.select( $"toc_org_CABC",$"toc_org_IABC",$"toc_org_NameABC",$"toc_org_CORPORATETAXNABC",$"toc_org_LASTUPDATEDATEABC",$"toc_con_stb_ENDDATEABC",$"toc_con_stb_STARTDATEABC",$"GRP_CNTRCT_STATUSABC",$"PRTY_TYPABC",$"PRTY_ROLEABC")
    writeContact.show(5, false)
    writeContact


  }

  def writeToOutput(writeContact: DataFrame, OutputValue: String ) =
    Writer.csvWriter(writeContact, OutputValue)
}
