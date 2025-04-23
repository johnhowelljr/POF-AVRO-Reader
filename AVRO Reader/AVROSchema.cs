// Base class for all Avro schema types
//
// I only support the record schema at the moment because it's all I needed.
// will update more as time goes on
//
using System.Text.Json;

public abstract class AvroSchema
{
	public abstract AvroSchemaType Type { get; }
}

public enum AvroSchemaType
{
	Null,
	Boolean,
	Int,
	Long,
	Float,
	Double,
	Bytes,
	String,
	Record,
	Enum,
	Array,
	Map,
	Union,
	Fixed,
	Unknown // Indicates parser issue or unhandled type
}

public class AvroPrimitiveSchema : AvroSchema
{
	private readonly AvroSchemaType _type;
	public override AvroSchemaType Type => _type;

	internal AvroPrimitiveSchema(AvroSchemaType type)
	{
		if (!IsPrimitive(type))
			throw new ArgumentException($"'{type}' is not a primitive Avro type.");
		_type = type;
	}

	private static bool IsPrimitive(AvroSchemaType type)
	{
		return type >= AvroSchemaType.Null && type <= AvroSchemaType.String;
	}

	public override string ToString() => Type.ToString().ToLowerInvariant();
}

public class AvroRecordSchema : AvroSchema
{
	public override AvroSchemaType Type => AvroSchemaType.Record;

	public string Name { get; internal set; }
	public string Namespace { get; internal set; }
	public string Doc { get; internal set; }
	public List<AvroField> Fields { get; internal set; }

	internal AvroRecordSchema()
	{
		Fields = new List<AvroField>();
	}
}

public class AvroField
{
	public string Name { get; internal set; }
	public AvroSchema Type { get; internal set; }
	public string Doc { get; internal set; }
	// public JsonElement? DefaultValue { get; internal set; }
}

public static class AvroSchemaParser
{
	/// <summary>
	/// Parses an Avro schema JSON string into C# AvroSchema objects using System.Text.Json.
	/// </summary>
	/// <param name="schemaJson">The Avro schema as a JSON string.</param>
	/// <returns>The root AvroSchema object.</returns>
	/// <exception cref="InvalidDataException">Thrown if the schema JSON is invalid or malformed according to Avro rules.</exception>
	public static AvroSchema ParseSchemaJson(string schemaJson)
	{
		if (string.IsNullOrWhiteSpace(schemaJson))
			throw new InvalidDataException("Schema JSON string is null or empty.");

		try
		{
			using (JsonDocument document = JsonDocument.Parse(schemaJson))
			{
				var schema = ParseSchemaElement(document.RootElement);

				// NOTE: This parser does NOT handle named type resolution.
				// It will throw InvalidDataException if it encounters a string that
				// is not a primitive type name and needs to be resolved as a named type.

				return schema;
			}
		}
		catch (JsonException ex)
		{
			throw new InvalidDataException($"Failed to parse Avro schema JSON: {ex.Message}", ex);
		}
		catch (InvalidDataException)
		{
			// Re-throw Avro schema validation errors
			throw;
		}
		catch (Exception ex)
		{
			throw new InvalidDataException($"An unexpected error occurred during schema parsing: {ex.Message}", ex);
		}
	}

	/// <summary>
	/// Recursive helper to parse a single JSON element representing an Avro schema.
	/// </summary>
	private static AvroSchema ParseSchemaElement(JsonElement element)
	{
		switch (element.ValueKind)
		{
			case JsonValueKind.String:
				// Could be a primitive type name (e.g., "string", "long") or a reference to a named type (e.g., "MyRecord")
				var typeName = element.GetString();
				if (string.IsNullOrEmpty(typeName))
					throw new InvalidDataException("Avro schema string value cannot be empty.");

				// Try to parse as a primitive type name
				var primitiveSchema = TryParsePrimitiveType(typeName);
				if (primitiveSchema != null)
					return primitiveSchema;

				// If not a primitive name, it MUST be a named type reference (like "MyRecord").
				// Our parser does *not* have a registry to resolve these named types.
				// Therefore, we must throw as we don't know what this name refers to.
				throw new InvalidDataException($"Encountered a string schema type '{typeName}' which is not a primitive and cannot be resolved as a named type reference (type resolution not implemented).");


			case JsonValueKind.Object:
				// Complex type definition (record, enum, array, map, fixed) OR Primitive type definition object {"type": "primitive"}
				if (!element.TryGetProperty("type", out JsonElement typeElement) || typeElement.ValueKind != JsonValueKind.String)
				{
					string rawJson = element.GetRawText();
					if (rawJson.Length > 100) rawJson = rawJson.Substring(0, 100) + "...";
					throw new InvalidDataException($"Avro schema object is missing the required 'type' string property. Object starts with: {rawJson}");
				}

				var typeNameInObject = typeElement.GetString().ToLowerInvariant();

				var primitiveSchemaFromObject = TryParsePrimitiveType(typeNameInObject);
				if (primitiveSchemaFromObject != null)
				{
					// It's a primitive schema defined as an object, e.g., {"type": "long"}
					// According to the spec, primitive type definitions have only the "type" property.
					// We could add validation here to ensure no other properties exist if needed for strictness.
					return primitiveSchemaFromObject;
				}

				// If it's not a primitive name, it must be a complex type name
				try
				{
					switch (typeNameInObject)
					{
						case "record":
							return ParseRecordSchema(element);
						default:
							throw new InvalidDataException($"Unknown or unsupported Avro complex schema type object: '{typeNameInObject}'.");
					}
				}
				catch (InvalidDataException ex)
				{
					// Add context about which complex type object had the error
					throw new InvalidDataException($"Error parsing Avro '{typeNameInObject}' schema object: {ex.Message}", ex);
				}


			default:
				string rawJsonDefault = element.GetRawText();
				if (rawJsonDefault.Length > 100) rawJsonDefault = rawJsonDefault.Substring(0, 100) + "...";
				throw new InvalidDataException($"Invalid JSON element type for Avro schema: {element.ValueKind}. Expected String, Array, or Object. Element starts with: {rawJsonDefault}");
		}
	}

	/// <summary>
	/// Tries to parse a string as a primitive Avro type name.
	/// Returns the schema if successful, otherwise returns null.
	/// </summary>
	private static AvroPrimitiveSchema TryParsePrimitiveType(string typeName)
	{
		// Use InvariantCultureIgnoreCase for case-insensitivity as per Avro spec recommendation
		switch (typeName.ToLowerInvariant())
		{
			case "null": return new AvroPrimitiveSchema(AvroSchemaType.Null);
			case "boolean": return new AvroPrimitiveSchema(AvroSchemaType.Boolean);
			case "int": return new AvroPrimitiveSchema(AvroSchemaType.Int);
			case "long": return new AvroPrimitiveSchema(AvroSchemaType.Long); // Correctly listed as a primitive
			case "float": return new AvroPrimitiveSchema(AvroSchemaType.Float);
			case "double": return new AvroPrimitiveSchema(AvroSchemaType.Double);
			case "bytes": return new AvroPrimitiveSchema(AvroSchemaType.Bytes);
			case "string": return new AvroPrimitiveSchema(AvroSchemaType.String);
			default:
				// Not a recognized primitive name
				return null;
		}
	}

	private static AvroRecordSchema ParseRecordSchema(JsonElement element)
	{
		var recordSchema = new AvroRecordSchema();

		// Required: name
		if (!element.TryGetProperty("name", out JsonElement nameElement) || nameElement.ValueKind != JsonValueKind.String || string.IsNullOrWhiteSpace(nameElement.GetString()))
		{
			var tempName = GetPropertyStringValue(element, "name") ?? "(unknown name)";
			throw new InvalidDataException($"Missing or invalid required 'name' string property for record schema (attempted name: '{tempName}').");
		}
		recordSchema.Name = nameElement.GetString();

		// Optional: namespace
		recordSchema.Namespace = GetPropertyStringValue(element, "namespace");

		// Optional: doc
		recordSchema.Doc = GetPropertyStringValue(element, "doc");

		// Required: fields
		if (!element.TryGetProperty("fields", out JsonElement fieldsElement) || fieldsElement.ValueKind != JsonValueKind.Array)
			throw new InvalidDataException($"Record '{recordSchema.Name}' is missing the required 'fields' array property.");

		recordSchema.Fields = new List<AvroField>();
		int fieldIndex = 0;
		foreach (var fieldElement in fieldsElement.EnumerateArray())
		{
			if (fieldElement.ValueKind != JsonValueKind.Object)
				throw new InvalidDataException($"Invalid element found in fields array ({fieldIndex}) for record '{recordSchema.Name}'. Expected an object, got {fieldElement.ValueKind}.");

			var field = new AvroField();

			// Required: field name
			if (!fieldElement.TryGetProperty("name", out JsonElement fieldNameElement) || fieldNameElement.ValueKind != JsonValueKind.String || string.IsNullOrWhiteSpace(fieldNameElement.GetString()))
			{
				var tempFieldName = GetPropertyStringValue(fieldElement, "name") ?? "(unknown field name)";
				throw new InvalidDataException($"Field at index {fieldIndex} in record '{recordSchema.Name}' is missing or has an invalid required 'name' string property (attempted name: '{tempFieldName}').");
			}
			field.Name = fieldNameElement.GetString();

			// Required: field type
			if (!fieldElement.TryGetProperty("type", out JsonElement fieldTypeElement))
			{
				throw new InvalidDataException($"Field '{field.Name}' at index {fieldIndex} in record '{recordSchema.Name}' is missing the required 'type' property.");
			}
			try
			{
				field.Type = ParseSchemaElement(fieldTypeElement); // Recursive call
			}
			catch (InvalidDataException ex)
			{
				throw new InvalidDataException($"Error parsing type for field '{field.Name}' at index {fieldIndex} in record '{recordSchema.Name}': {ex.Message}", ex);
			}


			// Optional: field doc
			field.Doc = GetPropertyStringValue(fieldElement, "doc");

			// Optional: default (complex to parse manually, skipping value extraction)
			// if (fieldElement.TryGetProperty("default", out JsonElement fieldDefaultElement))
			// {
			//    field.DefaultValue = fieldDefaultElement.Clone(); // Store the raw JSON element
			// }

			recordSchema.Fields.Add(field);
			fieldIndex++;
		}

		if (recordSchema.Fields.Count == 0)
			throw new InvalidDataException($"Record '{recordSchema.Name}' has an empty 'fields' array. Records must have fields.");

		return recordSchema;
	}


	// Helper to safely get an optional string property value
	private static string GetPropertyStringValue(JsonElement element, string propertyName)
	{
		if (element.TryGetProperty(propertyName, out JsonElement propertyElement) && propertyElement.ValueKind == JsonValueKind.String)
			return propertyElement.GetString();
		return null; // Return null if property is missing, not a string, or null JSON value
	}
}
