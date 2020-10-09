package org.rabbit.presto

import java.sql.ResultSet

import org.rabbit.config.PrestoConfig
import org.rabbit.util.DruidUtil.{execQuery, getConnection}

object PrestoJdbc {

  def main(args: Array[String]): Unit = {

    val connection = getConnection(PrestoConfig.url, "", PrestoConfig.user, "")
    println(connection)

//    val add2List = (rs: Option[ResultSet]) => {
//
//      var res: Seq[(Long, String, Int, Int, Int)] = Seq()
//      rs match {
//        case Some(result) =>
//          while (result.next()) {
//            val id = result.getLong("id")
//            val gid = result.getString("gid")
//            val price = result.getInt("price")
//            val srcuid = result.getInt("srcuid")
//            val dstuid = result.getInt("dstuid")
//            res = res :+ (id, gid, price, srcuid, dstuid)
//          }
//          res
//        case None => res
//      }
//    }

    val add2List = (rs: Option[ResultSet]) => {

      var res: Seq[(Int, Int, Int,java.math.BigDecimal,Long,java.math.BigDecimal,Long,java.math.BigDecimal,java.math.BigDecimal,Long,java.math.BigDecimal,java.math.BigDecimal,java.math.BigDecimal,java.math.BigDecimal)] = Seq()
      rs match {
        case Some(result) =>
          while (result.next()) {
            val created_at = result.getInt("created_at")
            val familyid = result.getInt("familyid")
            val dstuid = result.getInt("dstuid")
            val anchor_earnings = result.getBigDecimal("anchor_earnings")
            val ordinary_diamonds = result.getLong("ordinary_diamonds")
            val ordinary_family_earnings = result.getBigDecimal("ordinary_family_earnings")
            val message_diamonds = result.getLong("message_diamonds")
            val message_anchor_earnings = result.getBigDecimal("message_anchor_earnings")
            val message_family_earnings = result.getBigDecimal("message_family_earnings")
            val backpack_diamonds = result.getLong("backpack_diamonds")
            val backpack_anchor_earnings = result.getBigDecimal("backpack_anchor_earnings")
            val backpack_family_earnings = result.getBigDecimal("backpack_family_earnings")
            val lucky_gift_anchor_earnings = result.getBigDecimal("lucky_gift_anchor_earnings")
            val lucky_gift_family_earnings = result.getBigDecimal("lucky_gift_family_earnings")
            res = res :+ (created_at, familyid, dstuid,anchor_earnings, ordinary_diamonds, ordinary_family_earnings,message_diamonds,
              message_anchor_earnings,message_family_earnings,backpack_diamonds,backpack_anchor_earnings,backpack_family_earnings,
            lucky_gift_anchor_earnings,lucky_gift_family_earnings)
          }
          res
        case None => res
      }
    }

//    val sql = "select id,gid,price,srcuid,dstuid from hive.dwd.bill where year=2020 and month =9 and id < 12 limit 11"
//    execQuery(connection, sql)(add2List).foreach {
//      println(_)
//    }
//
//    val sql1 = "select id,gid,price,srcuid,dstuid from banban_mysql.banban.bill_202009 where id < 12 limit 11"
//    execQuery(connection, sql1)(add2List).foreach {
//      println(_)
//    }

    val sql2 =
      """
        |
        |with t1 as (
        |   SELECT
        |     familyid,
        | 		dstuid,
        | 		SUM(COALESCE(getmoney, 0)) AS anchor_earnings
        | 	FROM
        | 		hive.dwd.bill
        |   WHERE year = 2020 and month = 9 and  addtime >= 1601222400 and addtime < 1601308800
        |     and familyid > 0
        |   GROUP BY familyid, dstuid
        | ),
        |   t2 as (
        |     SELECT
        |       familyid,
        | 		  dstuid,
        | 		  SUM( price * count ) AS ordinary_diamonds,
        |       SUM(getbonus) AS ordinary_family_earnings
        | 	  FROM
        | 		  (select familyid, dstuid, price, count, getbonus, gid
        |         from hive.dwd.bill
        |         where year = 2020 and month = 9 and  addtime >= 1601222400 and addtime < 1601308800
        |           and familyid > 0) b
        | 		LEFT JOIN ( SELECT gid FROM hive.ods.config_gift_activity WHERE atype = 2 ) ca ON b.gid = ca.gid
        |     LEFT JOIN ( SELECT gid FROM hive.ods.config_giftlist WHERE subtype = 16 or subtype = 17) cg ON b.gid = cg.gid
        |     WHERE   ca.gid IS NULL and cg.gid IS NULL
        | 	  GROUP BY familyid,dstuid
        |   ),
        |   t3 as (
        |     SELECT
        |       familyid,
        |       dstuid,
        |       SUM( price ) AS message_diamonds,
        |       SUM( credit ) AS message_anchor_earnings,
        |       SUM(getbonus) AS message_family_earnings
        |     FROM hive.dwd.social_gift
        |     WHERE year = 2020 and month = 9 and ctime >= 1601222400 and ctime < 1601308800
        |       and familyid > 0
        |     GROUP BY familyid,dstuid
        |   ),
        |
        |  t5 as (
        |   SELECT
        |     familyid,
        | 		dstuid,
        |     SUM( gprice * count ) AS backpack_diamonds,
        | 		SUM(getmoney) AS backpack_anchor_earnings,
        |     SUM(getbonus) AS backpack_family_earnings
        | 	FROM hive.ods.config_giftlist  cc
        |   join (
        |     select gid, familyid, dstuid, price, count, getmoney, getbonus from
        | 		hive.dwd.bill
        |     WHERE year = 2020 and month = 9 and  addtime >= 1601222400 and addtime < 1601308800
        |      and bak = 'bak'
        |      and familyid > 0 ) bb ON cc.gid = bb.gid
        |
        |   GROUP BY familyid, dstuid
        |  ),
        |  t6 as (
        |   SELECT
        |        familyid,
        | 		   dstuid,
        | 		   SUM(COALESCE(getmoney,0)) AS firecracker_anchor_earnings,
        |        SUM(COALESCE(getbonus,0)) AS firecracker_family_earnings
        | 	   FROM
        | 		   (select familyid, dstuid, getmoney, getbonus, gid
        |         from hive.dwd.bill
        |         where year = 2020 and month = 9 and  addtime >= 1601222400 and addtime < 1601308800
        |           and familyid > 0) b
        | 		 JOIN ( SELECT gid FROM hive.ods.config_gift_activity WHERE atype = 2 ) ca ON b.gid = ca.gid
        | 	   GROUP BY familyid,dstuid
        |  ),
        |  t7 as (
        |   SELECT
        |           familyid,
        | 		      dstuid,
        | 		      SUM(COALESCE(getmoney,0)) AS crystal_anchor_earnings,
        |           SUM(COALESCE(getbonus,0)) AS crystal_family_earnings
        | 	      FROM
        | 		      (select familyid, dstuid, getmoney, getbonus, gid
        |             from hive.dwd.bill
        |             where year = 2020 and month = 9 and  addtime >= 1601222400 and addtime < 1601308800
        |               and familyid > 0) b
        |         JOIN ( SELECT gid FROM hive.ods.config_giftlist WHERE subtype = 17) cg ON b.gid = cg.gid
        | 	      GROUP BY familyid,dstuid
        |  ),
        |
        |  t_user as (
        |   select familyid, dstuid from t1
        |   union select familyid, dstuid from t3
        |  )
        |
        |
        | SELECT
        |   20200928 as created_at,
        |   t_user.*,
        |   COALESCE(t1.anchor_earnings,0) as anchor_earnings,
        |   COALESCE(t2.ordinary_diamonds,0) as ordinary_diamonds,
        |   COALESCE(t2.ordinary_family_earnings,0) as ordinary_family_earnings,
        | 	COALESCE(t3.message_diamonds,0) as message_diamonds,
        |   COALESCE(t3.message_anchor_earnings,0) as message_anchor_earnings,
        | 	COALESCE(t3.message_family_earnings, 0) as message_family_earnings,
        |
        |
        | 	COALESCE(t5.backpack_diamonds, 0) as backpack_diamonds,
        |   COALESCE(t5.backpack_anchor_earnings, 0) as backpack_anchor_earnings,
        |   COALESCE(t5.backpack_family_earnings, 0) as backpack_family_earnings,
        | 	(COALESCE(firecracker_anchor_earnings,0) + COALESCE(crystal_anchor_earnings,0)) as lucky_gift_anchor_earnings,
        | 	(COALESCE(firecracker_family_earnings,0) + COALESCE(crystal_family_earnings,0)) as lucky_gift_family_earnings
        |
        | FROM
        | 	t_user left join t1 on t_user.familyid = t1.familyid and t_user.dstuid = t1.dstuid
        |     left join t2 on t_user.familyid = t2.familyid and t_user.dstuid = t2.dstuid
        |     left join t3 on t_user.familyid = t3.familyid and t_user.dstuid = t3.dstuid
        |     left join t5 on t_user.familyid = t5.familyid and t_user.dstuid = t5.dstuid
        |     left join t6 on t_user.familyid = t6.familyid and t_user.dstuid = t6.dstuid
        |     left join t7 on t_user.familyid = t7.familyid and t_user.dstuid = t7.dstuid
        |     order by familyid,dstuid
        |
        |     limit 11
        |
        |""".stripMargin
    execQuery(connection, sql2)(add2List).foreach {
      println(_)
    }
  }

}
