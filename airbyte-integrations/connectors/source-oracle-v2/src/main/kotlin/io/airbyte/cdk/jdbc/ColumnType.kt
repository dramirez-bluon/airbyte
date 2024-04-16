package io.airbyte.cdk.jdbc

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.JsonSchemaPrimitiveUtil
import io.airbyte.protocol.models.JsonSchemaType
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZonedDateTime
import java.time.chrono.ChronoLocalDate
import java.time.format.DateTimeFormatter


sealed interface ColumnType {
    fun asJsonSchemaType(): JsonSchemaType

    fun asJsonSchema(): JsonNode =
        Jsons.jsonNode(asJsonSchemaType().jsonSchemaTypeMap)
}

data class ArrayColumnType(val item: ColumnType) : ColumnType {

    fun defaultElements(value: Any?): List<Any?> = when (value) {
        null -> listOf()
        is java.sql.Array -> (value.array as Iterable<*>).toList()
        is ArrayNode -> value.iterator().asSequence().toList()
        is Iterable<*> -> value.toList()
        else -> throw IllegalArgumentException("cannot process $value as an array type")
    }

    override fun asJsonSchemaType(): JsonSchemaType = JsonSchemaType
        .builder(JsonSchemaPrimitiveUtil.JsonSchemaPrimitive.ARRAY)
        .withItems(item.asJsonSchemaType())
        .build()
}

enum class LeafType(private val jsonSchemaType: JsonSchemaType) : ColumnType {
    BOOLEAN(JsonSchemaType.BOOLEAN) {
        override fun defaultMap(value: Any?): JsonNode = json.nullNode()
    },
    STRING(JsonSchemaType.STRING) {
        override fun defaultMap(value: Any?): JsonNode = when (value) {
            null -> json.nullNode()
            else -> json.textNode(value.toString())
        }
    },
    BINARY(JsonSchemaType.STRING_BASE_64) {
        override fun defaultMap(value: Any?): JsonNode = when (value) {
            null -> json.nullNode()
            is ByteArray -> json.binaryNode(value)
            else -> throw IllegalArgumentException("unsupported type ${value.javaClass}")
        }
    },
    DATE(JsonSchemaType.STRING_DATE) {
        override fun defaultMap(value: Any?): JsonNode = when (value) {
            null -> json.nullNode()
            is java.sql.Date -> json.textNode(value.toLocalDate().format(dateFormatter))
            is LocalDate -> json.textNode(value.format(dateFormatter))
            is LocalDateTime -> json.textNode(value.format(dateFormatter))
            is ChronoLocalDate -> json.textNode(value.format(dateFormatter))
            is OffsetDateTime -> json.textNode(value.format(dateFormatter))
            is ZonedDateTime -> json.textNode(value.format(dateFormatter))
            else -> json.textNode(value.toString())
        }
    },
    TIME_WITH_TIMEZONE(JsonSchemaType.STRING_TIME_WITH_TIMEZONE) {
        override fun defaultMap(value: Any?): JsonNode = when (value) {
            null -> json.nullNode()
            is java.sql.Time -> json.textNode(value.toLocalTime().format(timeTzFormatter))
            is LocalTime -> json.textNode(value.format(timeTzFormatter))
            is LocalDateTime -> json.textNode(value.format(timeTzFormatter))
            is ChronoLocalDate -> json.textNode(value.format(timeTzFormatter))
            is OffsetDateTime -> json.textNode(value.format(timeTzFormatter))
            is OffsetTime -> json.textNode(value.format(timeTzFormatter))
            is ZonedDateTime -> json.textNode(value.format(timeTzFormatter))
            else -> json.textNode(value.toString())
        }
    },
    TIME_WITHOUT_TIMEZONE(JsonSchemaType.STRING_TIME_WITHOUT_TIMEZONE) {
        override fun defaultMap(value: Any?): JsonNode = when (value) {
            null -> json.nullNode()
            is java.sql.Time -> json.textNode(value.toLocalTime().format(timeFormatter))
            is LocalTime -> json.textNode(value.format(timeFormatter))
            is LocalDateTime -> json.textNode(value.format(timeFormatter))
            is ChronoLocalDate -> json.textNode(value.format(timeFormatter))
            is OffsetDateTime -> json.textNode(value.format(timeFormatter))
            is OffsetTime -> json.textNode(value.format(timeFormatter))
            is ZonedDateTime -> json.textNode(value.format(timeFormatter))
            else -> json.textNode(value.toString())
        }
    },
    TIMESTAMP_WITH_TIMEZONE(JsonSchemaType.STRING_TIMESTAMP_WITH_TIMEZONE) {
        override fun defaultMap(value: Any?): JsonNode = when (value) {
            null -> json.nullNode()
            is java.sql.Time -> json.textNode(value.toLocalTime().format(timestampTzFormatter))
            is LocalTime -> json.textNode(value.format(timestampTzFormatter))
            is LocalDateTime -> json.textNode(value.format(timestampTzFormatter))
            is ChronoLocalDate -> json.textNode(value.format(timestampTzFormatter))
            is OffsetDateTime -> json.textNode(value.format(timestampTzFormatter))
            is OffsetTime -> json.textNode(value.format(timestampTzFormatter))
            is ZonedDateTime -> json.textNode(value.format(timestampTzFormatter))
            else -> json.textNode(value.toString())
        }
    },
    TIMESTAMP_WITHOUT_TIMEZONE(JsonSchemaType.STRING_TIMESTAMP_WITHOUT_TIMEZONE) {
        override fun defaultMap(value: Any?): JsonNode = when (value) {
            null -> json.nullNode()
            is java.sql.Time -> json.textNode(value.toLocalTime().format(timestampFormatter))
            is LocalTime -> json.textNode(value.format(timestampFormatter))
            is LocalDateTime -> json.textNode(value.format(timestampFormatter))
            is ChronoLocalDate -> json.textNode(value.format(timestampFormatter))
            is OffsetDateTime -> json.textNode(value.format(timestampFormatter))
            is OffsetTime -> json.textNode(value.format(timestampFormatter))
            is ZonedDateTime -> json.textNode(value.format(timestampFormatter))
            else -> json.textNode(value.toString())
        }
    },
    INTEGER(JsonSchemaType.INTEGER) {
        override fun defaultMap(value: Any?): JsonNode = when (value) {
            null -> json.nullNode()
            is Byte -> json.numberNode(value)
            is Short -> json.numberNode(value)
            is Int -> json.numberNode(value)
            is Long -> json.numberNode(value)
            is Float -> json.numberNode(java.math.BigDecimal.valueOf(value.toDouble()).toBigInteger())
            is Double -> json.numberNode(java.math.BigDecimal.valueOf(value).toBigInteger())
            is java.math.BigInteger -> json.numberNode(value)
            is java.math.BigDecimal -> json.numberNode(value.toBigInteger())
            else -> json.numberNode(java.math.BigInteger(value.toString()))
        }
    },
    NUMBER(JsonSchemaType.NUMBER) {
        override fun defaultMap(value: Any?): JsonNode = when (value) {
            null -> json.nullNode()
            is Byte -> json.numberNode(value)
            is Short -> json.numberNode(value)
            is Int -> json.numberNode(value)
            is Long -> json.numberNode(value)
            is Float -> json.numberNode(value)
            is Double -> json.numberNode(value)
            is java.math.BigInteger -> json.numberNode(value)
            is java.math.BigDecimal -> json.numberNode(value)
            else -> json.numberNode(java.math.BigDecimal(value.toString()))
        }
    },
    NULL(JsonSchemaType.NULL) {
        override fun defaultMap(value: Any?): JsonNode = json.nullNode()
    },
    JSONB(JsonSchemaType.JSONB) {
        override fun defaultMap(value: Any?): JsonNode = when (value) {
            null -> json.nullNode()
            is JsonNode -> value
            is ByteArray -> Jsons.deserialize(value)
            is CharSequence -> Jsons.deserialize(value.toString())
            else -> Jsons.jsonNode(value)
        }
    },
    ;

    abstract fun defaultMap(value: Any?): JsonNode

    override fun asJsonSchemaType(): JsonSchemaType = jsonSchemaType
}

private val json: JsonNodeFactory = MoreMappers.initMapper().nodeFactory
private val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
private val timeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS")
private val timeTzFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSXXX");
private val timestampFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
private val timestampTzFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX");

@JvmInline value class CatalogFieldSchema(val jsonSchemaProperties: JsonNode) {

    fun value(key: String): String = jsonSchemaProperties[key]?.asText() ?: ""
    fun type(): String = value("type")
    fun format(): String = value("format")
    fun airbyteType(): String = value("airbyte_type")

    fun asColumnType(): ColumnType = when(type()) {
        "array" -> ArrayColumnType(CatalogFieldSchema(jsonSchemaProperties["items"]).asColumnType())
        "null" -> LeafType.NULL
        "boolean" -> LeafType.BOOLEAN
        "number" -> when (airbyteType()) {
            "integer", "big_integer" -> LeafType.INTEGER
            else -> LeafType.NUMBER
        }
        "string" -> when(format()) {
            "date" -> LeafType.DATE
            "date-time" -> if (airbyteType() == "timestamp_with_timezone") {
                LeafType.TIMESTAMP_WITH_TIMEZONE
            } else {
                LeafType.TIMESTAMP_WITHOUT_TIMEZONE
            }
            "time" -> if (airbyteType() == "time_with_timezone") {
                LeafType.TIME_WITH_TIMEZONE
            } else {
                LeafType.TIME_WITHOUT_TIMEZONE
            }
            else -> if (value("contentEncoding") == "base64") {
                LeafType.BINARY
            } else {
                LeafType.STRING
            }
        }
        else -> LeafType.JSONB
    }
}
