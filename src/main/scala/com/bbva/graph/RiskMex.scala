package com.bbva.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext


// nohup spark-submit --class com.bbva.graph.RiskMex --driver-memory 50g RISK_GRAPHX_MX-assembly-1.0.jar &



/**
  * Created by Jorge Escandón on 30/01/17.
  */
object RiskMex {


  def main(args: Array[String]): Unit = {

    println("---------  EMPEZANDO -------")
    val sc = new SparkContext(new SparkConf().setAppName("Redex MEX").set("spark.yarn.executor.memoryOverhead", "1600")
      .set("spark.yarn.driver.memoryOverhead", "10000").set("spark.executor.memory","32g"))

    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    println("-------- CONFIGURANDO SPARK CONTEXT --------")
    val sqlContext = new HiveContext(sc)

    println("-------  BORRANDO TABLA ------")
    sqlContext.sql("DROP TABLE ri_grafos.sp_v0")



    //val df_cliente_destino = sqlContext.sql("SELECT * FROM ri_grafos.data_tmp")
    //val with_debt = sqlContext.sql("SELECT cliente_origen_id FROM ri_grafos.data_tmp where incumplimiento = 0")
    //val df_cliente_destino = sqlContext.sql("SELECT * FROM ri_grafos.data_tmp t WHERE cd_mes IN ('201606','201607','201608','201609','201610','201611','201612')  and (substr(cast(t.cliente_destino_id as string),1) not in ('0','1'))")
    //val with_debt = sqlContext.sql("SELECT DISTINCT(cast(cliente_origen_id as BIGINT)) cliente_origen_id FROM ri_grafos.data_tmp where incumplimiento = 0 AND cd_mes IN ('201606','201607','201608','201609','201610','201611','201612')  and (substr(cast(t.cliente_destino_id as string),1) not in ('0','1'))")

    // RECUPERANDO TODAS LAS OPERACIONES QUE SON DE CLIENTES INTERNOS BANCOMER
    val df_cliente_destino = sqlContext.sql("SELECT * FROM ri_grafos.data_tmp t WHERE (substr(cast(t.cliente_destino_id as string),1) not in ('0','1'))")
    // RECUPERANDO LOS IDS DE LOS CLIENTES CON MORA
    val with_debt = sqlContext.sql("SELECT DISTINCT(cast(cliente_origen_id as BIGINT)) cliente_origen_id FROM ri_grafos.data_tmp t where incumplimiento = 0 AND (substr(cast(t.cliente_destino_id as string),1) not in ('0','1'))")


    // GENERANDO LOS DATOS PARA QUE SE ADAPTEN A VERTICES Y ARISTAS
    val data_collected_dest = df_cliente_destino.collect()
    val data_vertices1 = data_collected_dest.flatMap {
      case (row) => Seq((row.getLong(1), ( row.getString(0), row.getString(2), row.getString(3), row.getDecimal(4).toString,  row.getLong(5).toString, row.getLong(6).toString)))
    }
    val data_vertices2 = data_collected_dest.flatMap {
      case (row) => Seq((row.getLong(6), ( row.getString(0), row.getString(2), row.getString(3), row.getDecimal(4).toString,  row.getLong(5).toString, row.getLong(1).toString)))
    }

    val merged_array_1 = data_vertices1 ++ data_vertices2
    val data_edges = data_collected_dest.flatMap {
      value =>
        Seq(Edge(value.getLong(1), value.getLong(6), "Transaccion"))
    }

    // LIMPIANDO LOS VERTICES PARA QUE SEAN ÚNICOS
    val merged_array_0 = merged_array_1.distinct

    // CREANDO EL GRAFO
    val data_node: RDD[(VertexId, (String, String, String, String, String, String))] = sc.parallelize(merged_array_0)
    val edges: RDD[Edge[String]] = sc.parallelize(data_edges)
    val graph_bbva_customer = Graph(data_node, edges)
/*

    val only_internal_accounts = graph.vertices.filter {
      case(id, (a1,a2,a3,a4,a5, a6)) => id > 1
    }
    val graph_bbva_customer = Graph(only_internal_accounts, edges)
    */

    // EL GRAFO GENERADO SE METE EN MEMORIA CACHE
    graph_bbva_customer.cache()

    // SE RECUPERAN LAS CONEXIONES ENTRANTES DE LOS VERTICES
    val customers_bbva_without_connected_in = graph_bbva_customer.ops.collectNeighbors(EdgeDirection.In)
    val all_ids = customers_bbva_without_connected_in.flatMap {
      current =>
        Seq(current._1)
    }

    // HASHMAPS PARA LOS CLIENTES CON MORA Y EL PAR DE CAMINO MÁS CORTO
    val debt_ids  = scala.collection.mutable.HashMap.empty[VertexId, String]
    val pair_shortest_id  = scala.collection.mutable.HashMap.empty[Long, Long]

    // LA ESTRCUTRUA DE DATOS QUE CONTIENE TODOS LOS IDS DE LOS USUARIOS CON MORA
    val with_debt_collected = with_debt.collect()

    // SE LLENA EL MAPA DE IDS CON MORA PARA SU ACCESO LINEAL O(1)
    with_debt_collected.foreach { ids => debt_ids += ids.getLong(0) -> "0" }


    // EL CAMINO MAS CORTO
    val AllShortPathsResult = ShortestPathsCustom.run(graph_bbva_customer, all_ids.toArray)

    // SE FILTRA LOS VERTICES QUE NO TINENE NINGUN VECINO
    val filteredShortestPath = AllShortPathsResult.vertices.filter {
      case(a, b) => b != 0
    }

    // SE RECUPERAN LOS VERTICES PARA PODER FILTRAR EL CAMINO MAS CORTO A LA MORA
    val all_final_vertices = filteredShortestPath.collect()


    /*
    all_final_vertices.foreach {
      current_row => {
        val output = current_row._2.toList
        if(!output.isEmpty) {
          val filtered_min_values = output.filter {
            // FILTRA EL CAMINO 0 ES DECIR EL CAMINO HACÍA SI MISMO
            case (a, b) => b != 0
          }
          val only_debts = filtered_min_values.filter {
            case (a, b) => debt_ids.exists(_._1 == a)
          }
          println("DEBTS"+only_debts)
          if (!only_debts.isEmpty) {
            val final_min = only_debts.minBy(_._2)._2
            pair_shortest_id += ((current_row._1, final_min))
            println("DEBT NO EMPTY"+(current_row._1, final_min))
          } else {
            pair_shortest_id += ((current_row._1, 0))
            println("DEBT EMPTY"+(current_row._1, 0))
          }
        }
      }
    }
    */

    val readyRDD = all_final_vertices.map {
      case(id, mapa) => {
        mapa.map {
          case(sub_id, sp) => {
            (id, sub_id, sp)
          }
        }
      }
    }

    val readyRDD0 = readyRDD.flatMap {
      eachList => {
        eachList.map {
          case(id, sub, sp) => (id, sub, sp)
        }
      }
    }

    val finalReadyRDD = readyRDD0.filter {
      case(id, sub, sp) => sp != 0
    }

    import sqlContext.implicits._

    val dfrdd = sc.parallelize(readyRDD0)

    val df = dfrdd.toDF("origen_id_sp", "destino_id_sp", "sp_a_destino")

    df.printSchema()
    df.registerTempTable("sp_0")
    with_debt.registerTempTable("only_ids_with_debt")

    val relation_with_debt = sqlContext.sql("SELECT sp.origen_id_sp, sp_a_destino FROM sp_0 sp, only_ids_with_debt db WHERE sp.destino_id_sp=db.cliente_origen_id")
    relation_with_debt.registerTempTable("final_results")

    sqlContext.sql("CREATE TABLE ri_grafos.sp_v0_only_debts TBLPROPERTIES ('parquet.compress'='SNAPPY') AS SELECT * FROM final_results ")

    sqlContext.sql("CREATE TABLE ri_grafos.sp_v0_test TBLPROPERTIES ('parquet.compress'='SNAPPY') AS SELECT origen_id_sp, MIN(sp_a_destino) FROM final_results GROUP BY origen_id_sp")

    println("----------    END Saved in database  ----------")


  }

}