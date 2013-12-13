import org.junit.Test
import org.neo4j.cypher.ExecutionEngineHelper
import org.neo4j.cypher.internal.compilator.v2_0._
import org.neo4j.kernel.api.index.IndexDescriptor


class VisitorTest extends ExecutionEngineHelper {
  @Test
  def should_apa() {

    (0 to 1000) foreach (x =>
      createLabeledNode(Map("NAME" -> "Andres", "AGE" -> x), "PERSON"))

    graph.createIndex("PERSON", "NAME")

    val value = new Literal("Andres")
    val seek = new IdxSeek(new IndexDescriptor(0, 0), value, new RegisterCreator(1, 1), 0)
    val propertyRead = new NodePropertyRead(new NodeFromRegister(0), 1)
    val projection = new Projection(seek, 0, propertyRead)

    val visitor = new Visitor {
      def visit(register: Register) {
        // Do nothing
      }
    }

    (0 to 5) foreach {
      x =>
      // Warmp up
        graph.inTx {
          projection.accept(visitor, statement)
        }

        execute("match (n:PERSON {name:'ANDRES'}) RETURN n.AGE").toList
    }


    (0 to 100) foreach {
      x =>
      // Measure
        val t1 = System.nanoTime()
        graph.inTx {
          projection.accept(visitor, statement)
        }
        val t2 = System.nanoTime()
        println("New code: " + ((t2 - t1) * 1e-6) + " milliseconds")

        val t3 = System.nanoTime()
        execute("match (n:PERSON {name:'ANDRES'}) RETURN n.AGE").toList
        val t4 = System.nanoTime()
        println("Old code: " + ((t4 - t3) * 1e-6) + " milliseconds")
    }
  }
}