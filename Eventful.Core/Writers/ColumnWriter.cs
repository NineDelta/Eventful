using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Eventful.Core.Writers
{
	internal class ColumnWriter : IDisposable
	{
		private readonly string _path;
		private readonly FileStream _columnStream;
		private readonly BinaryWriter _columnWriter;

		public Action<object> Write { get; private set; }

		public ColumnWriter(string path, PropertyInfo column, Dictionary<Type, Action<object, BinaryWriter>> customWriters)
		{
			_path = path;
			_columnStream = new FileStream(path + column.Name + ".col", FileMode.Append, FileAccess.Write, FileShare.Read);
			_columnWriter = new BinaryWriter(_columnStream);
			
			Write = CreateWriter(column, customWriters);
		}

		private Action<object> CreateWriter(PropertyInfo column, Dictionary<Type, Action<object, BinaryWriter>> customWriters)
		{
			// Note: haven't ported range compression, run length encoding, dictionary, variable length encoding etc..
            // Bottle neck appears to be CPU rather than IO at this stage, and compression will worsen this.

			if (customWriters.ContainsKey(column.PropertyType))
			{
				var writer = customWriters[column.PropertyType];
				return o => writer(column.GetValue(o), _columnWriter);
			}

			if (column.PropertyType == typeof(bool))
			{
				return o => _columnWriter.Write((bool)column.GetValue(o));
			}
			if (column.PropertyType == typeof(bool?))
			{
				return o =>
				{
					var value = (bool?)column.GetValue(o);
					_columnWriter.Write(value.HasValue);
					if (value.HasValue) _columnWriter.Write(value.Value);
				};
			}
			if (column.PropertyType == typeof(byte))
			{
				return o => _columnWriter.Write((byte)column.GetValue(o));
			}
			if (column.PropertyType == typeof(byte?))
			{
				return o =>
				{
					var value = (byte?)column.GetValue(o);
					_columnWriter.Write(value.HasValue);
					if (value.HasValue) _columnWriter.Write(value.Value);
				};
			}
			if (column.PropertyType == typeof(sbyte))
			{
				return o => _columnWriter.Write((sbyte)column.GetValue(o));
			}
			if (column.PropertyType == typeof(sbyte?))
			{
				return o =>
				{
					var value = (sbyte?)column.GetValue(o);
					_columnWriter.Write(value.HasValue);
					if (value.HasValue) _columnWriter.Write(value.Value);
				};
			}
			if (column.PropertyType == typeof(short))
			{
				return o => _columnWriter.Write((short)column.GetValue(o));
			}
			if (column.PropertyType == typeof(short?))
			{
				return o =>
				{
					var value = (short?)column.GetValue(o);
					_columnWriter.Write(value.HasValue);
					if (value.HasValue) _columnWriter.Write(value.Value);
				};
			}
			if (column.PropertyType == typeof(ushort))
			{
				return o => _columnWriter.Write((ushort)column.GetValue(o));
			}
			if (column.PropertyType == typeof(ushort?))
			{
				return o =>
				{
					var value = (ushort?)column.GetValue(o);
					_columnWriter.Write(value.HasValue);
					if (value.HasValue) _columnWriter.Write(value.Value);
				};
			}
			if (column.PropertyType == typeof(int))
			{
				return o => _columnWriter.Write((int)column.GetValue(o));
			}
			if (column.PropertyType == typeof(int?))
			{
				return o =>
				{
					var value = (int?)column.GetValue(o);
					_columnWriter.Write(value.HasValue);
					if (value.HasValue) _columnWriter.Write(value.Value);
				};
			}
			if (column.PropertyType == typeof(uint))
			{
				return o => _columnWriter.Write((uint)column.GetValue(o));
			}
			if (column.PropertyType == typeof(uint?))
			{
				return o =>
				{
					var value = (uint?)column.GetValue(o);
					_columnWriter.Write(value.HasValue);
					if (value.HasValue) _columnWriter.Write(value.Value);
				};
			}
			if (column.PropertyType == typeof(long))
			{
				return o => _columnWriter.Write((long) column.GetValue(o));
			}
			if (column.PropertyType == typeof(long?))
			{
				return o =>
				{
					var value = (long?)column.GetValue(o);
					_columnWriter.Write(value.HasValue);
					if (value.HasValue) _columnWriter.Write(value.Value);
				};
			}
			if (column.PropertyType == typeof(ulong))
			{
				return o => _columnWriter.Write((ulong)column.GetValue(o));
			}
			if (column.PropertyType == typeof(ulong?))
			{
				return o =>
				{
					var value = (ulong?)column.GetValue(o);
					_columnWriter.Write(value.HasValue);
					if (value.HasValue) _columnWriter.Write(value.Value);
				};
			}
			if (column.PropertyType == typeof(Guid))
			{
				return o => _columnWriter.Write(((Guid)column.GetValue(o)).ToByteArray());
			}
			if (column.PropertyType == typeof(Guid?))
			{
				return o =>
				{
					var value = (Guid?)column.GetValue(o);
					_columnWriter.Write(value.HasValue);
					if (value.HasValue) _columnWriter.Write(value.Value.ToByteArray());
				};
			}
			if (column.PropertyType == typeof(decimal))
			{
				return o =>
				{					
					var handle = GCHandle.Alloc((decimal)column.GetValue(o), GCHandleType.Pinned);

					try
					{
						var bytes = new byte[16];
						Marshal.Copy(handle.AddrOfPinnedObject(), bytes, 0, 16);
						_columnWriter.Write(bytes);
					}
					finally
					{
						if (handle.IsAllocated)
							handle.Free();
					}
				};
			}
			if (column.PropertyType == typeof(decimal?))
			{
				return o =>
				{
					var value = (decimal?)column.GetValue(o);
					_columnWriter.Write(value.HasValue);
					if (value.HasValue)
					{
						var handle = GCHandle.Alloc(value.Value, GCHandleType.Pinned);

						try
						{
							var bytes = new byte[16];
							Marshal.Copy(handle.AddrOfPinnedObject(), bytes, 0, 16);
							_columnWriter.Write(bytes);
						}
						finally
						{
							if (handle.IsAllocated)
								handle.Free();
						}
					}
				};
			}
			if (column.PropertyType == typeof(double))
			{
				return o => _columnWriter.Write((double)column.GetValue(o));
			}
			if (column.PropertyType == typeof(double?))
			{
				return o =>
				{
					var value = (double?)column.GetValue(o);
					_columnWriter.Write(value.HasValue);
					if (value.HasValue) _columnWriter.Write(value.Value);
				};
			}
			if (column.PropertyType == typeof(DateTime))
			{
				return o => _columnWriter.Write(((DateTime)column.GetValue(o)).Ticks);
			}
			if (column.PropertyType == typeof(DateTime?))
			{
				return o =>
				{
					var value = (DateTime?)column.GetValue(o);
					_columnWriter.Write(value.HasValue);
					if (value.HasValue) _columnWriter.Write(value.Value.Ticks);
				};
			}
			if (column.PropertyType == typeof(DateTimeOffset))
			{
				return o =>
				{
					var value = (DateTimeOffset)column.GetValue(o);
					_columnWriter.Write(value.Ticks);
					_columnWriter.Write(value.TimeOfDay.Ticks);
				};
			}
			if (column.PropertyType == typeof(DateTimeOffset?))
			{
				return o =>
				{
					var value = (DateTimeOffset?)column.GetValue(o);
					_columnWriter.Write(value.HasValue);
					if (value.HasValue) _columnWriter.Write(value.Value.Ticks);
					if (value.HasValue) _columnWriter.Write(value.Value.TimeOfDay.Ticks);
				};
			}
			if (column.PropertyType.IsEnum)
			{
				var underlyingType = Enum.GetUnderlyingType(column.PropertyType);

				if (underlyingType == typeof(byte))
					return o => _columnWriter.Write((byte)column.GetValue(o));

				if (underlyingType == typeof(short))
					return o => _columnWriter.Write((short)column.GetValue(o));

				if (underlyingType == typeof(int))
					return o => _columnWriter.Write((int)column.GetValue(o));
					
				throw new NotSupportedException("Unsupported underlying enum type: " + underlyingType.FullName);
			}
			if (Nullable.GetUnderlyingType(column.PropertyType)?.IsEnum == true)
			{
				var underlyingType = Enum.GetUnderlyingType(Nullable.GetUnderlyingType(column.PropertyType));

				if (underlyingType == typeof(byte))
					return o =>
					{
						var value = column.GetValue(o);
						_columnWriter.Write(value != null);
						if (value != null) _columnWriter.Write((byte)value);
					};

				if (underlyingType == typeof(short))
					return o =>
					{
						var value = column.GetValue(o);
						_columnWriter.Write(value != null);
						if (value != null) _columnWriter.Write((short)value);
					};

				if (underlyingType == typeof(int))
					return o =>
					{
						var value = column.GetValue(o);
						_columnWriter.Write(value != null);
						if (value != null) _columnWriter.Write((int)value);
					};
			}
			if (column.PropertyType == typeof(string))
			{
				return o => _columnWriter.Write((string)column.GetValue(o));
			}
			if (column.PropertyType.IsArray)
			{
				var writer = new TypeWriter(_path + column.Name + ".", column.PropertyType.GetElementType(), customWriters);
				return o =>
				{
					var value = (Array)column.GetValue(o);
					_columnWriter.Write(value != null);
					if (value != null)
					{
						_columnWriter.Write((ushort)value.Length);
						for (var i = 0; i < value.Length; i++)
						{
							writer.Write(value.GetValue(i));
						}
					}
				};
			}
			if (column.PropertyType.IsGenericType && ((column.PropertyType.GetGenericTypeDefinition() == typeof(List<>)) || (column.PropertyType.GetGenericTypeDefinition() == typeof(Collection<>))))
			{
				var writer = new TypeWriter(_path + column.Name + ".", column.PropertyType.GetGenericArguments().Single(), customWriters);
				return o =>
				{
					var value = (IList)column.GetValue(o);
					_columnWriter.Write(value != null);
					if (value != null)
					{
						_columnWriter.Write((ushort)value.Count);
						for (var i = 0; i < value.Count; i++)
						{
							writer.Write(value[i]);
						}
					}
				};
			}
			if (!column.PropertyType.IsValueType)
			{
				var writer = new TypeWriter(_path + column.Name + ".", column.PropertyType, customWriters);
				return o =>
				{
					var value = column.GetValue(o);
					_columnWriter.Write(value != null);
					if (value != null) writer.Write(value);
				};
			}

			throw new NotSupportedException("Unsupported column type: " + column.PropertyType.FullName);
		}

		public void Dispose()
		{
			_columnWriter.Dispose();
			_columnStream.Dispose();
		}
	}
}