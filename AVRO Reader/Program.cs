using System.Dynamic;
using System.IO;
using System.Text;
using System;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Runtime.CompilerServices;

namespace AVRO_Reader
{
	internal class Program
	{
		static void Main(string[] args)
		{
			AVROFile avroFile = new AVROFile();
			avroFile.Read(@"C:\Users\johnh\Downloads\syncInMeta.avro");
		}
	}

	public class AVROFile
	{
		private byte[] m_avroHeader = { 0x4F, 0x62, 0x6A, 0x01 };// Obj1
		private FileStream m_fs = null;
		private BinaryReader m_br = null;
		private long m_blockStart = 0;

		// meta data
		private Dictionary<string,byte[]>	m_meta = new Dictionary<string,byte[]>();

		public AVROFile()
		{
		}

		public AVROBlock ReadNextBlock ()
		{
			return null;
		}

		public int BlockCount ()
		{
			return 0;
		}

		public bool Read(string filename)
		{
			if (File.Exists(filename))
			{
				this.m_fs = new FileStream(filename, FileMode.Open, FileAccess.Read);
				this.m_br = new BinaryReader(this.m_fs, Encoding.UTF8, leaveOpen: true);

				byte[] magicHeader = this.m_br.ReadBytes(4);
				if (magicHeader.SequenceEqual(this.m_avroHeader))
				{

					long entryCount = 0;
					while ((entryCount = ReadVarLong()) != 0 )
					{
						if (entryCount < 0)
							entryCount = -entryCount;

						for ( int index = 0; index < entryCount; index++ )
						{
							long keyLenth = ReadVarLong();
							byte[] keys = this.m_br.ReadBytes((int)keyLenth);
							long valueLength = ReadVarLong();
							byte[] values = this.m_br.ReadBytes((int)valueLength);

							// store it
							this.m_meta.Add(Encoding.UTF8.GetString(keys), values);
						}
					}

					// that's the header, read the sync marker
					byte[] syncMarker = this.m_br.ReadBytes(16);
					// btw, its random in avro we just need to make sure we could read it
					if (syncMarker.Length != 16)
						throw new Exception("Could not read sync marker");

					// have a schema? we must since it's all I support at the moment
					if ( this.m_meta.ContainsKey("avro.schema"))
					{
						string schema = Encoding.UTF8.GetString ( this.m_meta["avro.schema"]);

						AvroSchema parsedSchema = AvroSchemaParser.ParseSchemaJson(schema);
						PrintSchemaInfo(parsedSchema, "  ");
					}


					long blockTotal = 0;
					while ( this.m_br.BaseStream.Position < this.m_br.BaseStream.Length )
					{
						long blockCount = ReadVarLong();
						long blockSize = ReadVarLong();

						//  skip over
						byte[] blockData = this.m_br.ReadBytes((int)blockSize);


						// sync marker
						syncMarker = this.m_br.ReadBytes(16);
						blockTotal += blockCount;

					}

					return true;
				}
				else
				{
					this.m_fs.Close();
					this.m_br.Close();
					return false;
				}

			}
			else
				return false;
		}

		// Helper method to print the parsed schema structure (recursive)
		private static void PrintSchemaInfo(AvroSchema schema, string indent)
		{
			if (schema == null) return;

			Console.Write($"{indent}Type: {schema.Type}");

			if (schema is AvroPrimitiveSchema)
				Console.WriteLine(); // Primitive types have no more details here
			else if (schema is AvroRecordSchema recordSchema)
			{
				if (!string.IsNullOrEmpty(recordSchema.Name)) Console.Write($", Name: {recordSchema.Name}");
				if (!string.IsNullOrEmpty(recordSchema.Namespace)) Console.Write($", Namespace: {recordSchema.Namespace}");
				Console.WriteLine();
				Console.WriteLine($"{indent}  Fields ({recordSchema.Fields.Count}):");
				foreach (var field in recordSchema.Fields)
					PrintSchemaInfo(field.Type, indent + "      "); // Recurse
			}
			else
				Console.WriteLine(); // Fallback for Unknown or other unhandled types
		}
		private long ReadVarLong()
		{
			long value = 0;
			int shift = 0;
			byte b;

			// Read bytes until the high bit is not set
			do
			{
				if (shift >= 64)
					throw new InvalidDataException("Malformed var-long: value too large or too many bytes.");

				// Ensure there are bytes left to read
				if (this.m_br.BaseStream.Position >= this.m_br.BaseStream.Length)
					throw new EndOfStreamException("Stream ended unexpectedly while reading var-long.");

				b = this.m_br.ReadByte();
				value |= (long)(b & 0x7F) << shift;
				shift += 7;

			} while ((b & 0x80) != 0); // Loop while the most significant bit is set

			 long decoded = value >> 1; // Arithmetic shift (sign-preserving for negative numbers)
			 if ((value & 1) != 0)
			     decoded = ~decoded; // Bitwise NOT
			 return decoded;
		}
		/*
		public override bool TryGetMember(GetMemberBinder binder, out object? result)
		{
			string name = binder.Name.ToLower();
			return this.m_properties.TryGetValue(name, out result);
		}
		public override bool TrySetMember(SetMemberBinder binder, object? value)
		{
			this.m_properties[binder.Name.ToLower()] = value;
			return true;
		}
		*/

	}

	public class AVROBlock : DynamicObject
	{
	}
}
