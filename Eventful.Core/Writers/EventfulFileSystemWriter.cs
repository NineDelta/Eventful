using System;
using System.Collections.Generic;
using System.IO;

namespace Eventful.Core.Writers
{
	public class EventfulFileSystemWriter : IDisposable
    {
	    private readonly Dictionary<string, ushort> _knownTypes = new Dictionary<string, ushort>();
	    private readonly TypeWriter[] _typeWriters = new TypeWriter[ushort.MaxValue];
		private readonly string _folder;

	    private readonly FileStream _knownTypesStream;
	    private readonly BinaryWriter _knownTypesWriter;

	    private readonly FileStream _recordsStream;
	    private readonly BinaryWriter _recordsWriter;


		public EventfulFileSystemWriter(string folder)
	    {
		    _folder = folder;

		    Directory.CreateDirectory(folder);

		    _knownTypesStream = new FileStream(_folder + "\\types.idx", FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
		    _knownTypesWriter = new BinaryWriter(_knownTypesStream);

			_recordsStream = new FileStream(_folder + "\\sequence.log", FileMode.Append, FileAccess.Write, FileShare.Read);
			_recordsWriter = new BinaryWriter(_recordsStream);

			using (var stream = new FileStream(_folder + "\\types.idx", FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
			using (var knownTypesReader = new BinaryReader(stream))
			{
				while (knownTypesReader.BaseStream.Position != knownTypesReader.BaseStream.Length)
				{
					var index = knownTypesReader.ReadUInt16();
					var path = knownTypesReader.ReadString();

					_knownTypes.Add(path, index);
				}
			}
	    }

	    private readonly Dictionary<Type, Action<object, BinaryWriter>> _customWriters =
		    new Dictionary<Type, Action<object, BinaryWriter>>();

		public void RegisterWriter<TType>(Action<TType, BinaryWriter> writer)
	    {
		    _customWriters.Add(typeof(TType), (o, bw) => writer((TType)o, bw));
		}

	    private uint writtenEventCount = 0;

	    public void Write(object e)
	    {
		    var typeId = GetOrCreateKnownType(e.GetType());
			var typeWriter = _typeWriters[typeId];

		    if (typeWriter == null)
		    {
			    typeWriter = new TypeWriter(_folder + "\\" + e.GetType().FullName + "\\", e.GetType(), _customWriters);
			    _typeWriters[typeId] = typeWriter;
		    }

		    typeWriter.Write(e, writtenEventCount);

			// Lastly, commit to records index
			_recordsWriter.Write(typeId);

		    writtenEventCount++;
	    }

	    private ushort GetOrCreateKnownType(Type type)
	    {
		    if (_knownTypes.TryGetValue(type.FullName, out ushort typeId))
			    return typeId;

		    typeId = (ushort)_knownTypes.Count;
		    _knownTypes.Add(type.FullName, typeId);

			_knownTypesWriter.Write(typeId);
		    _knownTypesWriter.Write(type.FullName);

		    Directory.CreateDirectory(_folder + "\\" + type.FullName);

		    _knownTypesWriter.Flush();

			return typeId;
	    }

	    public void Dispose()
	    {
		    _recordsWriter.Dispose();
			_recordsStream.Dispose();

		    _knownTypesWriter.Dispose();
		    _knownTypesStream.Dispose();

		    foreach (var type in _typeWriters)
		    {
			    if (type == null)
				    continue;

			    type.Dispose();
		    }
		}
    }
}
