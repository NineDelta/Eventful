using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using Eventful.Core.Readers.DataTypes;
using Eventful.Core.Readers.ProxyGeneration;

namespace Eventful.Core
{
	internal class MiscReader
	{
		private readonly FileStream _columnStream;
		private readonly BinaryReader _columnReader;
		private readonly string _path;

		public Action<object> Read { get; private set; }

		public MiscReader(string path, PropertyInfo column, Dictionary<Type, Func<BinaryReader, object>> customReaders)
		{
			_path = path;

		    _columnStream = CreateStream(path, column);
		    _columnReader = new BinaryReader(_columnStream);

		    Read = CreateReader(column, customReaders);
        }

	    public static FileStream CreateStream(string path, PropertyInfo column)
	    {
	        var columnPath = path + column.Name + ".col";

            return new FileStream(columnPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 65536, FileOptions.SequentialScan);
        }

	    public FileStream InnerStream
	    {
	        get { return _columnStream; }
	    }

	    public BinaryReader InnerReader
	    {
	        get { return _columnReader; }
	    }

		private Action<object> CreateReader(PropertyInfo column, Dictionary<Type, Func<BinaryReader, object>> customReaders)
		{
            // Column didn't exist when write occurred.
		    if (_columnStream == null)
		        return o => { };

			if (customReaders.ContainsKey(column.PropertyType))
			{
				var reader = customReaders[column.PropertyType];
				return o => column.SetValue(o, reader(_columnReader));
			}

			var paramObject = Expression.Parameter(typeof(object), "o");
			var typedObject = Expression.Convert(paramObject, column.DeclaringType);

			if (column.PropertyType == typeof(bool))
			{
				return o => column.SetValue(o, _columnReader.ReadBoolean());
			}
			if (column.PropertyType == typeof(bool?))
			{
				return o => column.SetValue(o, _columnReader.ReadBoolean() ? _columnReader.ReadBoolean() : (bool?)null);
			}
			if (column.PropertyType == typeof(byte))
			{
				return o => column.SetValue(o, _columnReader.ReadByte());
			}
			if (column.PropertyType == typeof(byte?))
			{
				return o => column.SetValue(o, _columnReader.ReadBoolean() ? _columnReader.ReadByte() : (byte?)null);
			}
			if (column.PropertyType == typeof(sbyte))
			{
				return o => column.SetValue(o, _columnReader.ReadSByte());
			}
			if (column.PropertyType == typeof(sbyte?))
			{
				return o => column.SetValue(o, _columnReader.ReadBoolean() ? _columnReader.ReadSByte() : (sbyte?)null);
			}
			if (column.PropertyType == typeof(short))
			{
				return o => column.SetValue(o, _columnReader.ReadInt16());
			}
			if (column.PropertyType == typeof(short?))
			{
				return o => column.SetValue(o, _columnReader.ReadBoolean() ? _columnReader.ReadInt16() : (short?)null);
			}
			if (column.PropertyType == typeof(ushort))
			{
				return o => column.SetValue(o, _columnReader.ReadUInt16());
			}
			if (column.PropertyType == typeof(ushort?))
			{
				return o => column.SetValue(o, _columnReader.ReadBoolean() ? _columnReader.ReadUInt16() : (ushort?)null);
			}
			if (column.PropertyType == typeof(int))
			{
				return o => column.SetValue(o, _columnReader.ReadInt32());
			}
			if (column.PropertyType == typeof(int?))
			{
				return o => column.SetValue(o, _columnReader.ReadBoolean() ? _columnReader.ReadInt32() : (int?)null);
			}
			if (column.PropertyType == typeof(uint))
			{
				return o => column.SetValue(o, _columnReader.ReadUInt32());
			}
			if (column.PropertyType == typeof(uint?))
			{
				return o => column.SetValue(o, _columnReader.ReadBoolean() ? _columnReader.ReadUInt32() : (uint?)null);
			}
			if (column.PropertyType == typeof(long))
			{
				return o => column.SetValue(o, _columnReader.ReadInt64());
			}
			if (column.PropertyType == typeof(long?))
			{
				return o => column.SetValue(o, _columnReader.ReadBoolean() ? _columnReader.ReadInt64() : (long?)null);
			}
			if (column.PropertyType == typeof(ulong))
			{
				return o => column.SetValue(o, _columnReader.ReadUInt64());
			}
			if (column.PropertyType == typeof(ulong?))
			{
				return o => column.SetValue(o, _columnReader.ReadBoolean() ? _columnReader.ReadUInt64() : (ulong?)null);
			}
			if (column.PropertyType == typeof(Guid))
			{
				return o => column.SetValue(o, new Guid(_columnReader.ReadBytes(16)));
			}
			if (column.PropertyType == typeof(Guid?))
			{
				return o => column.SetValue(o, _columnReader.ReadBoolean() ? new Guid(_columnReader.ReadBytes(16)) : (Guid?)null);
			}
			if (column.PropertyType == typeof(decimal))
			{
                var reader = new DecimalReader(_columnStream);
                var paramValue = Expression.Parameter(typeof(decimal), "d");
                var call = Expression.Call(typedObject, column.GetSetMethod(), paramValue);

                var setter = Expression.Lambda<Action<object, decimal>>(call, paramObject, paramValue).Compile();

                return o => setter(o, reader.ReadDecimal(0));
            }
			if (column.PropertyType == typeof(decimal?))
			{
				return o => column.SetValue(o, _columnReader.ReadBoolean() ? _columnReader.ReadDecimal() : (decimal?)null);
			}
			if (column.PropertyType == typeof(double))
			{
				return o => column.SetValue(o, _columnReader.ReadDouble());
			}
			if (column.PropertyType == typeof(double?))
			{
				return o => column.SetValue(o, _columnReader.ReadBoolean() ? _columnReader.ReadDouble() : (double?)null);
			}
			if (column.PropertyType == typeof(DateTime))
			{
				return o => column.SetValue(o, new DateTime(_columnReader.ReadInt64()));
			}
			if (column.PropertyType == typeof(DateTime?))
			{
				return o => column.SetValue(o, _columnReader.ReadBoolean() ? new DateTime(_columnReader.ReadInt64()) : (DateTime?)null);
			}
			if (column.PropertyType == typeof(DateTimeOffset))
			{
				return o => column.SetValue(o, new DateTimeOffset(_columnReader.ReadInt64(), new TimeSpan(_columnReader.ReadInt64())));
			}
			if (column.PropertyType == typeof(DateTimeOffset?))
			{
				return o => column.SetValue(o, _columnReader.ReadBoolean() ? new DateTimeOffset(_columnReader.ReadInt64(), new TimeSpan(_columnReader.ReadInt64())) : (DateTimeOffset?)null);
			}
			if (column.PropertyType == typeof(string))
			{
			    return o => column.SetValue(o, _columnReader.ReadString());
			}
			if (column.PropertyType.IsEnum)
			{
				var underlyingType = Enum.GetUnderlyingType(column.PropertyType);

				if (underlyingType == typeof(byte))
					return o => column.SetValue(o, _columnReader.ReadByte());

				if (underlyingType == typeof(short))
					return o => column.SetValue(o, _columnReader.ReadInt16());

				if (underlyingType == typeof(int))
					return o => column.SetValue(o, _columnReader.ReadInt32());

				throw new NotSupportedException("Unsupported underlying enum type: " + underlyingType.FullName);

			}
			if (Nullable.GetUnderlyingType(column.PropertyType)?.IsEnum == true)
			{
				var underlyingType = Enum.GetUnderlyingType(Nullable.GetUnderlyingType(column.PropertyType));

				if (underlyingType == typeof(byte))
					return o => column.SetValue(o, _columnReader.ReadBoolean() ? (object)_columnReader.ReadByte() : null);

				if (underlyingType == typeof(short))
					return o => column.SetValue(o, _columnReader.ReadBoolean() ? (object)_columnReader.ReadInt16() : null);

				if (underlyingType == typeof(int))
					return o => column.SetValue(o, _columnReader.ReadBoolean() ? (object)_columnReader.ReadInt32() : null);

				throw new NotSupportedException("Unsupported underlying enum type: " + underlyingType.FullName);
			}
			if (column.PropertyType.IsArray)
			{
				var reader = new TypeReaderFactory(_path + column.Name + ".", column.PropertyType.GetElementType(), customReaders);
				return o =>
				{
					var isNonNull = _columnReader.ReadBoolean();

					if (!isNonNull)
					{
						column.SetValue(o, null);
					}
					else
					{
						var length = _columnReader.ReadUInt16();
						var array = (Array)Activator.CreateInstance(column.PropertyType, length);

						for (int i = 0; i < length; i++)
						{
							array.SetValue(reader.ReadNew(), i);
						}

						column.SetValue(o, array);
					}
				};
			}
			if (!column.PropertyType.IsValueType)
			{
				var reader = new TypeReaderFactory(_path + column.Name + ".", column.PropertyType, customReaders);
				return o =>
				{
					var isNonNull = _columnReader.ReadBoolean();
					column.SetValue(o, isNonNull ? reader.ReadNew() : null);
				};
			}

			throw new Exception("Unsupported column type " + column.PropertyType.Name);
		}

		public void Dispose()
		{
		    if (_columnReader != null)
		    {
		        _columnReader.Dispose();
		        _columnStream.Dispose();
		    }
		}
	}
}