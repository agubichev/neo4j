/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_1.planner.execution

import org.neo4j.cypher.internal.compiler.v2_1.pipes._
import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.CantHandleQueryException
import org.neo4j.cypher.internal.compiler.v2_1.runtime.{RootNode, Expression, Operator}
import org.neo4j.cypher.internal.compiler.v2_1.runtime.operators._
import com.oracle.truffle.api.frame._
import org.neo4j.cypher.internal.compiler.v2_1.runtime.expressions.{RelationshipTypeId, PropertyKeyId,
NodeGetPropertyFactory, LabelId, HasLabelFactory}
import org.neo4j.cypher.internal.compiler.v2_1.runtime.expressions.literals.{BooleanLiteral, LongLiteral, IntegerLiteral, StringLiteral}
import com.oracle.truffle.api.{Truffle, CallTarget}
import org.neo4j.cypher.internal.compiler.v2_1.runtime.expressions.operators.{OrFactory, EqualsFactory,
GreaterThanFactory, AndFactory, LessThanFactory, NotFactory}
import org.neo4j.cypher.internal.compiler.v2_1.runtime.expressions.slots.{ReadParam, ProjectFactory, PushFactory, ReadFactory, WriteFactory}
import org.neo4j.cypher.javacompat
import com.oracle.truffle.api.nodes.NodeUtil
import java.util
import org.neo4j.cypher.javacompat.ProfilerStatistics
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexUniqueSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.compiler.v2_1.Monitors
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Expand
import org.neo4j.cypher.internal.compiler.v2_1.ast.RelTypeName
import org.neo4j.cypher.internal.compiler.v2_1.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.PipeInfo
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Selection
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Apply
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.compiler.v2_1.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.SingleRow
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Projection

case class ProxyProjectionPipe(operator: Operator, projection: Array[String],
                               descriptor: FrameDescriptor)
                              (implicit val monitor: PipeMonitor) extends Pipe {
  val paramSlots = descriptor.getSlots.toArray.map(_.asInstanceOf[FrameSlot]).filter { slot =>
    val identifier = slot.getIdentifier.asInstanceOf[String]
    identifier.startsWith("__param__")
  }
  val rootNode: RootNode = new RootNode(operator, descriptor, projection.length, paramSlots)
  val target: CallTarget = Truffle.getRuntime.createCallTarget( rootNode )
  val paramNames = paramSlots.map { slot =>
    slot.getIdentifier.asInstanceOf[String].substring("__param__".length)
  }

  def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val paramValues = paramNames.map(state.params.getOrElse(_, null)).asInstanceOf[Array[Object]]
    val result = target.call( state.db, state.query.getStatement, paramValues ).asInstanceOf[Array[Object]]
    result.iterator.map { rowRef: AnyRef =>
      val row = rowRef.asInstanceOf[Array[Object]]
      ExecutionContext.empty.newFrom(projection.zip(row).toSeq)
    }
  }

  def exists(pred: Pipe => Boolean) = pred(this)

  def planDescription: PlanDescription = {
    val that = this
    new PlanDescription {
      def find(name: String): Seq[PlanDescription] = Seq.empty

      def map(f: (PlanDescription) => PlanDescription): PlanDescription = this

      override def andThen(pipe: Pipe, name: String, arguments: Argument*) = PlanDescriptionImpl(pipe, name, SingleChild(this), arguments)

      def children = NoChildren

      def args = Seq.empty

      def pipe: Pipe = that

      lazy val asJava: javacompat.PlanDescription = new javacompat.PlanDescription {
        def getProfilerStatistics: ProfilerStatistics = null

        def hasProfilerStatistics: Boolean = false

        def getChildren: util.List[javacompat.PlanDescription] = new util.ArrayList

        def getChild(name: String): javacompat.PlanDescription = null

        def cd(names: String*): javacompat.PlanDescription = null

        def getArguments: util.Map[String, AnyRef] = new util.HashMap

        def getName: String = "ProxyProjectionPipe"

        override def toString: String = NodeUtil.printTreeToString(that.rootNode)
      }

      def render(builder: StringBuilder, separator: String, levelSuffix: String): StringBuilder = {
        render( builder )
      }

      def render(builder: StringBuilder): StringBuilder = {
        builder.append( NodeUtil.printTreeToString(that.rootNode) )
      }

      def addArgument(arg: Argument): PlanDescription =
        throw new UnsupportedOperationException("Cannot add arguments")

      def arguments: Seq[Argument] = Seq.empty

      def toSeq: Seq[PlanDescription] = Seq(this)

      def name: String = "ProxyProjectionPipe"
    }
  }

  def symbols = SymbolTable()
}

class TruffleExecutionPlanBuilder(monitors: Monitors) {
  def build(plan: LogicalPlan): PipeInfo = {
    val updating = false

    val descriptor = new FrameDescriptor()
    def addFrameSlot(name: String, kind: FrameSlotKind): FrameSlot =
      descriptor.addFrameSlot(name, kind)
    def findFrameSlot(name: String): FrameSlot =
      descriptor.findFrameSlot(name)
    def addNodeFrameSlot(name: String): FrameSlot =
      descriptor.addFrameSlot("__node__" + name, FrameSlotKind.Long)
    def addOptionalNodeFrameSlot(name: String): FrameSlot =
      descriptor.addFrameSlot("__node__" + name, FrameSlotKind.Object)
    def findNodeFrameSlot(name: String): FrameSlot =
      descriptor.findFrameSlot("__node__" + name)
    def findOrAddNodeFrameSlot(name: String): FrameSlot =
      descriptor.findOrAddFrameSlot("__node__" + name, FrameSlotKind.Long)
    def findOrAddOptionalNodeFrameSlot(name: String): FrameSlot =
      descriptor.findOrAddFrameSlot("__node__" + name, FrameSlotKind.Object)
    def addRelationshipFrameSlot(name: String): FrameSlot =
      descriptor.addFrameSlot("__rel__" + name, FrameSlotKind.Long)
    def addOptionalRelationshipFrameSlot(name: String): FrameSlot =
      descriptor.addFrameSlot("__rel__" + name, FrameSlotKind.Object)
    def findRelationshipFrameSlot(name: String): FrameSlot =
      descriptor.findFrameSlot("__rel__" + name)
    var i = 0
    def addAnonymousFrameSlot(kind: FrameSlotKind): FrameSlot = {
      i += 1
      descriptor.addFrameSlot("__anonymous__" + i, kind)
    }

    def buildExpression(e: ast.Expression): Expression = e match {
      case l: ast.BooleanLiteral =>
        new BooleanLiteral(l.value.asInstanceOf[java.lang.Boolean])
      case l: ast.IntegerLiteral =>
        new LongLiteral(l.value)
      case l: ast.StringLiteral =>
        new StringLiteral(l.value)
      case ast.Not(right) =>
        NotFactory.create(buildExpression(right))
      case ast.NotEquals(left, right) =>
        NotFactory.create(EqualsFactory.create(buildExpression(left), buildExpression(right)))
      case ast.And(left, right) =>
        AndFactory.create(buildExpression(left), buildExpression(right))
      case ast.Or(left, right) =>
        OrFactory.create(buildExpression(left), buildExpression(right))
      case ast.GreaterThan(left, right) =>
        GreaterThanFactory.create(buildExpression(left), buildExpression(right))
      case ast.LessThan(left, right) =>
        LessThanFactory.create(buildExpression(left), buildExpression(right))
      case ast.Equals(left, right) =>
        EqualsFactory.create(buildExpression(left), buildExpression(right))
      case ast.Property(identifier, ast.PropertyKeyName(propertyName)) =>
        NodeGetPropertyFactory.create(
          buildExpression(identifier),
          new PropertyKeyId(propertyName)
        )
      case ast.HasLabels(identifier, Seq(ast.LabelName(labelName))) =>
        HasLabelFactory.create(
          buildExpression(identifier),
          new LabelId(labelName)
        )
      case ast.Identifier(name) =>
        val slot = Option(findNodeFrameSlot(name))
          .orElse(Option(findRelationshipFrameSlot(name)))
          .getOrElse(findFrameSlot(name))
        ReadFactory.create(slot)
      case ast.Parameter(name) =>
        new ReadParam( descriptor.findOrAddFrameSlot( "__param__" + name, FrameSlotKind.Object ) )
      case _ =>
        throw new CantHandleQueryException()
    }

    var projection: Array[String] = null
    def buildOperator(plan: LogicalPlan): Operator = {
      plan match {
        case SingleRow(_) =>
          new NullOperator(addAnonymousFrameSlot(FrameSlotKind.Boolean))
        case CartesianProduct(left, right) =>
          new CartesianProductOperator(buildOperator(left), buildOperator(right))
        case AllNodesScan(IdName(id)) =>
          new AllNodeScanOperator(
            addNodeFrameSlot(id),
            addAnonymousFrameSlot(FrameSlotKind.Object)
          )
        case NodeByLabelScan(IdName(id), label) =>
          val idExp = label match {
            case Left(a) => new LabelId(a)
            case Right(b) => new IntegerLiteral(b.id)
          }
          new NodeByLabelScanOperator(
            idExp,
            addNodeFrameSlot(id),
            addAnonymousFrameSlot(FrameSlotKind.Object)
          )
        case NodeByIdSeek(IdName(id), Seq(nodeIdExpr)) =>
          new NodeByIdSeekOperator(
            buildExpression(nodeIdExpr),
            addNodeFrameSlot(id),
            addAnonymousFrameSlot(FrameSlotKind.Boolean)
          )
        case NodeIndexSeek(IdName(id), labelId, propertyKeyId, valueExpr) =>
          new NodeByIndexSeekOperator(
            labelId.id, propertyKeyId.id, buildExpression( valueExpr ),
            addNodeFrameSlot(id),
            addAnonymousFrameSlot(FrameSlotKind.Object)
          )
        case NodeIndexUniqueSeek(IdName(id), labelId, propertyKeyId, valueExpr) =>
          new NodeByUniqueIndexSeekOperator(
            labelId.id, propertyKeyId.id, buildExpression( valueExpr ),
            addNodeFrameSlot(id),
            addAnonymousFrameSlot(FrameSlotKind.Object)
          )
        case Selection(predicates, left) =>
          predicates.foldLeft(buildOperator(left)) {
            case (op, expr) =>
              new SelectionOperator(op, buildExpression(expr))
          }
        case Expand(left, IdName(from), dir, Seq(RelTypeName(relType)), IdName(to), IdName(relName), SimplePatternLength) =>
          val relSlot = addRelationshipFrameSlot(relName)
          new JumpOperator(
            new ExpandOperator(
              buildOperator(left),
              new RelationshipTypeId(relType),
              findNodeFrameSlot(from),
              relSlot,
              addAnonymousFrameSlot(FrameSlotKind.Object),
              dir
            ),
            relSlot,
            findOrAddNodeFrameSlot(to),
            dir
          )
        case Expand(left, IdName(from), dir, Seq(RelTypeName(relType)), IdName(to), IdName(relName), VarPatternLength(min, max)) =>
          val relSlot = addRelationshipFrameSlot(relName)
          new VarLengthExpandOperator(
            buildOperator(left),
            min,
            max.getOrElse(null).asInstanceOf[Integer],
            new RelationshipTypeId(relType),
            findNodeFrameSlot(from),
            relSlot,
            addNodeFrameSlot(to),
            addAnonymousFrameSlot(FrameSlotKind.Object),
            addAnonymousFrameSlot(FrameSlotKind.Object),
            dir
          )
        case OptionalExpand(left, IdName(fromName), dir, Seq(RelTypeName(relType)), IdName(toName), IdName(relName), SimplePatternLength, predicates) =>
          val predicate: ast.Expression = predicates.foldLeft[ast.Expression](ast.True()(null)) { ast.And(_, _)(null) }
          new OptionalExpandOperator(
            buildOperator(left),
            new RelationshipTypeId(relType),
            findNodeFrameSlot(fromName),
            findOrAddOptionalNodeFrameSlot(toName),
            addOptionalRelationshipFrameSlot(relName),
            addAnonymousFrameSlot(FrameSlotKind.Object),
            dir,
            buildExpression( predicate )
          )
        case Projection(left, expressions) =>
          projection = expressions.keys.toArray
          expressions.foldLeft(buildOperator(left)) {
            case (op, (key, astExpr)) =>
              val expr = buildExpression( astExpr )
              val kind = astExpr match {
                case ast.Identifier(name) =>
                  val slot = Option(findNodeFrameSlot(name))
                    .orElse(Option(findRelationshipFrameSlot(name)))
                    .getOrElse(findFrameSlot(name))
                  slot.getKind
                case _ =>
                  FrameSlotKind.Object
              }
              new Foreach(op, WriteFactory.create( expr, addFrameSlot( key, kind ) ) )
          }
        case NodeHashJoin(IdName(joinId), left, right) =>
          new NodeHashJoinOperator(
            buildOperator(left),
            buildOperator(right),
            findNodeFrameSlot(joinId),
            addAnonymousFrameSlot(FrameSlotKind.Object),
            addAnonymousFrameSlot(FrameSlotKind.Object)
          )
        case Apply(outer, inner) =>
          new ApplyOperator(
            buildOperator(outer),
            buildOperator(inner),
            addAnonymousFrameSlot(FrameSlotKind.Boolean)
          )
        case SemiApply(outer, inner) =>
          new SemiApplyOperator(
            buildOperator(outer),
            buildOperator(inner)
          )
        case Sort(left, sortItems) =>
          new SortOperator(
            buildOperator(left),
            sortItems.map(desc => findFrameSlot(desc.id)).toArray,
            addAnonymousFrameSlot(FrameSlotKind.Object)
          )
        case Skip(left, literal: ast.IntegerLiteral) =>
          new SkipOperator(
            buildOperator(left),
            literal.value
          )
        case Limit(left, literal: ast.IntegerLiteral) =>
          new LimitOperator(
            buildOperator(left),
            literal.value,
            addAnonymousFrameSlot(FrameSlotKind.Object)
          )
        case _ =>
          throw new CantHandleQueryException()
      }
    }

    val topLevelOp = buildOperator(plan)
    val projectionSlot = descriptor.addFrameSlot( "projection", FrameSlotKind.Object )
    val projectionOp = projection.foldLeft(topLevelOp) {
      case (op, name) =>
        val slot = Option(findNodeFrameSlot(name))
          .orElse(Option(findRelationshipFrameSlot(name)))
          .getOrElse(findFrameSlot(name))
        new Foreach(op, PushFactory.create( ReadFactory.create( slot ), projectionSlot ))
    }

    implicit val monitor = monitors.newMonitor[PipeMonitor]()
    val topLevelPipe = ProxyProjectionPipe(projectionOp, projection, descriptor)
    PipeInfo(topLevelPipe, updating, None)
  }
}
