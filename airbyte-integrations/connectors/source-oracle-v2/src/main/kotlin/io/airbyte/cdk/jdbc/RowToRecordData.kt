package io.airbyte.cdk.jdbc

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.cdk.read.DataColumn
import io.airbyte.cdk.read.StreamSpec
import io.airbyte.commons.json.Jsons
import java.util.function.Function

class RowToRecordData(
    val sourceOperations: SourceOperations,
    val streamSpec: StreamSpec,
) : Function<List<Any?>, JsonNode> {

    private val mappers: List<(Any?) -> JsonNode> =
        streamSpec.dataColumns.map(::buildMapper)

    private fun buildMapper(dataColumn: DataColumn): (Any?) -> JsonNode {
        val mapperInner = buildMapperRecursive(dataColumn.type)
        return { v: Any? ->
            try {
                mapperInner(v)
            } catch (e: Exception) {
                throw RuntimeException("${dataColumn.metadata.name} value $v not suitable " +
                    "for ${dataColumn.type}", e)
            }
        }
    }

    private fun buildMapperRecursive(type: ColumnType): (Any?) -> JsonNode {
        when(type) {
            is LeafType -> return { v: Any? ->
                type.defaultMap(sourceOperations.mapLeafColumnValue(type, v))
            }
            is ArrayColumnType -> {
                val itemMapper = buildMapperRecursive(type.item)
                return { v: Any? ->
                    Jsons.arrayNode().apply {
                        for (e in type.defaultElements(sourceOperations.mapArrayColumnValue(v))) {
                            add(itemMapper(e))
                        }
                    }
                }
            }
        }
    }

    override fun apply(row: List<Any?>): JsonNode {
        val objectNode: ObjectNode = Jsons.emptyObject() as ObjectNode
        row.forEachIndexed { i, v ->
            objectNode.set<ObjectNode>(streamSpec.dataColumns[i].metadata.name, mappers[i](v))
        }
        return objectNode
    }
}
