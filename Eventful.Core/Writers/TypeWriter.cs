using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;

namespace Eventful.Core.Writers
{
	internal class TypeWriter : IDisposable
	{
		private readonly FileStream _sequenceStream;
		private readonly BinaryWriter _sequenceWriter;
		public ColumnWriter[] Columns { get; set; }

		public TypeWriter(string path, Type type, Dictionary<Type, Action<object, BinaryWriter>> customWriters)
		{
			_sequenceStream = new FileStream(path + "sequence.log", FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
			_sequenceWriter = new BinaryWriter(_sequenceStream);

			var properties = type
				.GetProperties(BindingFlags.Instance | BindingFlags.Public)
				.Where(p => p.CanRead && p.CanWrite);

			Columns = properties.Select(p => new ColumnWriter(path, p, customWriters)).ToArray();
		}

		public void Write(object o, uint index)
		{
			_sequenceWriter.Write(index);

			Write(o);
		}

		public void Write(object o)
		{
			for (var i = 0; i < Columns.Length; i++)
			{
				Columns[i].Write(o);
			}
		}

		public void Dispose()
		{
			foreach (var column in Columns)
			{
				column.Dispose();
			}

			_sequenceWriter.Dispose();
			_sequenceStream.Dispose();
		}
	}
}