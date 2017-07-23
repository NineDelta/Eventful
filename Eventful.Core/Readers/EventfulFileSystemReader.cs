using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Eventful.Core.Readers.ProxyGeneration;

namespace Eventful.Core.Readers
{
    public interface IEventfulReader
    {
        IEnumerable<object> Read(params Type[] eventTypes);
        void Invoke(Dictionary <Type, Action<object>> handlers);
    }

    public class EventfulFileSystemReader : IEventfulReader
    {
        private readonly string _folder;

        private readonly Dictionary<string, ushort> _typeIdsMap = new Dictionary<string, ushort>();

        public EventfulFileSystemReader(string folder)
        {
            _folder = folder;


            // Known Type Mapping
            using (var stream = new FileStream(_folder + "\\types.idx", FileMode.Open, FileAccess.Read,
                FileShare.ReadWrite))
            using (var reader = new BinaryReader(stream))
            {
                var length = reader.BaseStream.Length;
                while (reader.BaseStream.Position != length)
                {
                    var typeId = reader.ReadUInt16();
                    var typeName = reader.ReadString();

                    _typeIdsMap.Add(typeName, typeId);
                }
            }
        }

        private readonly Dictionary<Type, Func<BinaryReader, object>> _customReaders =
            new Dictionary<Type, Func<BinaryReader, object>>();

        public void RegisterReader<TType>(Func<BinaryReader, TType> reader)
        {
            _customReaders.Add(typeof(TType), br => (object) reader(br));
        }

        private class ReaderProgress
        {
            public UInt32 NextSequenceId { get; set; }
            public TypeReaderFactory Reader { get; set; }
            public ReaderProgress Next { get; internal set; }

        }

        public IEnumerable<object> Read(params Type[] eventTypes)
        {
            var typeReaders = CreateTypeReaders(eventTypes.ToDictionary(t => t, t => (Action<object>)(_ => { })));
            var relevantTypeReaders = new ReaderProgressSinglyLinkedList(typeReaders);

            var nextReader = relevantTypeReaders.First;
            var subsequentReader = nextReader.Next;

            while (true)
            {
                // Use first reader, unless a subsequent reader in priority queue has an older message.
                if (subsequentReader != null && nextReader.NextSequenceId > subsequentReader.NextSequenceId)
                {
                    //  Scan until we can find new insert point
                    relevantTypeReaders.RemoveFirst();

                    var node = subsequentReader;
                    while (true)
                    {
                        if (node.Next == null)
                        {
                            relevantTypeReaders.AddAfter(node, nextReader);
                            break;
                        }

                        if (nextReader.NextSequenceId < node.Next.NextSequenceId)
                        {
                            relevantTypeReaders.AddAfter(node, nextReader);
                            break;
                        }

                        node = node.Next;
                    }

                    // Get next
                    nextReader = relevantTypeReaders.First;
                    subsequentReader = nextReader.Next;
                }

                var instance = nextReader.Reader.ReadPooled();

                yield return instance;

                var nextSeqId = nextReader.Reader.ReadNextSequenceId();

                if (nextSeqId == null)
                {
                    relevantTypeReaders.RemoveFirst();

                    if (relevantTypeReaders.First == null)
                        break;

                    nextReader = relevantTypeReaders.First;
                    subsequentReader = nextReader.Next;
                }
                else
                {
                    nextReader.NextSequenceId = nextSeqId.Value;
                }
            }

            foreach (var type in typeReaders)
            {
                type?.Dispose();
            }
        }

        public void Invoke(Dictionary<Type, Action<object>> handlers)
        {
            var typeReaders = CreateTypeReaders(handlers);
            var relevantTypeReaders = new ReaderProgressSinglyLinkedList(typeReaders);

            var nextReader = relevantTypeReaders.First;
            var subsequentReader = nextReader.Next;

            while (true)
            {
                // Use first reader, unless a subsequent reader in priority queue has an older message.
                if (subsequentReader != null && nextReader.NextSequenceId > subsequentReader.NextSequenceId)
                {
                    //  Scan until we can find new insert point
                    relevantTypeReaders.RemoveFirst();

                    var node = subsequentReader;
                    while (true)
                    {
                        if (node.Next == null)
                        {
                            relevantTypeReaders.AddAfter(node, nextReader);
                            break;
                        }

                        if (nextReader.NextSequenceId < node.Next.NextSequenceId)
                        {
                            relevantTypeReaders.AddAfter(node, nextReader);
                            break;
                        }

                        node = node.Next;
                    }

                    // Get next
                    nextReader = relevantTypeReaders.First;
                    subsequentReader = nextReader.Next;
                }

                var handler = nextReader.Reader.Handler;
                var instance = nextReader.Reader.ReadPooled();

                handler(instance);
                
                var nextSeqId = nextReader.Reader.ReadNextSequenceId();

                if (nextSeqId == null)
                {
                    relevantTypeReaders.RemoveFirst();

                    if (relevantTypeReaders.First == null)
                        break;

                    nextReader = relevantTypeReaders.First;
                    subsequentReader = nextReader.Next;
                }
                else
                {
                    nextReader.NextSequenceId = nextSeqId.Value;
                }
            }

            foreach (var type in typeReaders)
            {
                type?.Dispose();
            }
        }

        //IEnumerable<object> Read(params Type[] eventTypes);

        //void Invoke(params KeyValuePair<Type, Action<object>>[] handlers);

        private List<TypeReaderFactory> CreateTypeReaders(Dictionary<Type, Action<object>> handlers)
        {
            var knownProxies = new Dictionary<Type, List<Type>>();

            var typesMap = new Type[ushort.MaxValue];
            var typeReaders = new TypeReaderFactory[ushort.MaxValue];

            foreach (var type in handlers)
            {
                var originalTypes = knownProxies.ContainsKey(type.Key) ? knownProxies[type.Key] : new List<Type> { type.Key };

                foreach (var originalType in originalTypes)
                {
                    if (_typeIdsMap.ContainsKey(originalType.FullName))
                    {
                        var typeId = _typeIdsMap[originalType.FullName];
                        var path = _folder + "\\" + originalType.FullName + "\\";

                        typesMap[typeId] = type.Key;
                        typeReaders[typeId] = new TypeReaderFactory(path, type.Key, _customReaders)
                        {
                            Handler = handlers[type.Key]
                        };
                    }
                }
            }

            return typeReaders.Where(t => t != null).ToList();
        }


        //internal void Dispatch(Dictionary<Type, List<Type>> knownProxies, Dictionary<Type, Action<object>> handlers)
        //{
        //    var typesMap = new Type[ushort.MaxValue];
        //    var typeReaders = new TypeReaderFactory[ushort.MaxValue];

        //    foreach (var type in handlers.Keys)
        //    {
        //        var originalTypes = knownProxies.ContainsKey(type) ? knownProxies[type] : new List<Type> { type };

        //        foreach (var originalType in originalTypes)
        //        {
        //            if (_typeIdsMap.ContainsKey(originalType.FullName))
        //            {
        //                var typeId = _typeIdsMap[originalType.FullName];
        //                var path = _folder + "\\" + originalType.FullName + "\\";

        //                typesMap[typeId] = type;
        //                typeReaders[typeId] = new TypeReaderFactory(path, type, _customReaders)
        //                {
        //                    Handler = handlers[type]
        //                };
        //            }
        //        }
        //    }

        //    // Special case single event type - we don't care about interleaved events in this case.
        //    //if (handlers.Keys.Count == 1)
        //    //{
        //    //	var typeReader = typeReaders.Single(t => t != null);
        //    //	var handler = typeReader.Handler;

        //    //	var count = typeReader.ReadCount();
        //    //	for (var i = 0; i < count; i++)
        //    //	{
        //    //		var instance = typeReader.ReadPooled();

        //    //		handler(instance);
        //    //	}
        //    //}
        //    //else


        //    if (handlers.Keys.Count < 10) // Ideally, do this on % of records and number of different types. Need stats to do that though.
        //    {
        //        var relevantTypeReaders = new ReaderProgressSinglyLinkedList(typeReaders.Where(t => t != null).Select(r =>
        //            new ReaderProgress
        //            {
        //                NextSequenceId = r.ReadNextSequenceId() ?? UInt32.MaxValue, // TODO: Remove if nothing left
        //                Reader = r
        //            }).OrderBy(s => s.NextSequenceId));

        //        var nextReader = relevantTypeReaders.First;
        //        var subsequentReader = nextReader.Next;

        //        while (true)
        //        {
        //            // Use first reader, unless a subsequent reader in priority queue has an older message.
        //            if (subsequentReader != null && nextReader.NextSequenceId > subsequentReader.NextSequenceId)
        //            {
        //                //  Scan until we can find new insert point
        //                relevantTypeReaders.RemoveFirst();

        //                var node = subsequentReader;
        //                while (true)
        //                {
        //                    if (node.Next == null)
        //                    {
        //                        relevantTypeReaders.AddAfter(node, nextReader);
        //                        break;
        //                    }

        //                    if (nextReader.NextSequenceId < node.Next.NextSequenceId)
        //                    {
        //                        relevantTypeReaders.AddAfter(node, nextReader);
        //                        break;
        //                    }

        //                    node = node.Next;
        //                }

        //                // Get next
        //                nextReader = relevantTypeReaders.First;
        //                subsequentReader = nextReader.Next;
        //            }

        //            var handler = nextReader.Reader.Handler;
        //            var instance = nextReader.Reader.ReadPooled();

        //            handler(instance);

        //            var nextSeqId = nextReader.Reader.ReadNextSequenceId();

        //            if (nextSeqId == null)
        //            {
        //                relevantTypeReaders.RemoveFirst();

        //                if (relevantTypeReaders.First == null)
        //                    break;

        //                nextReader = relevantTypeReaders.First;
        //                subsequentReader = nextReader.Next;

        //                //if (relevantTypeReaders.Length == 1)
        //                //{
        //                //    minReader = relevantTypeReaders[0];
        //                //    handler = minReader.Reader.Handler;

        //                //    while (true)
        //                //    {
        //                //        instance = minReader.Reader.ReadPooled();

        //                //        handler(instance);

        //                //        if (minReader.Reader.ReadNextSequenceId() == null)
        //                //            break;
        //                //    }

        //                //    break;
        //                //}
        //            }
        //            else
        //            {
        //                nextReader.NextSequenceId = nextSeqId.Value;
        //            }
        //        }
        //    }
        //    else
        //    {
        //        using (var recordsStream = new FileStream(_folder + "\\sequence.log", FileMode.Open, FileAccess.Read,
        //            FileShare.ReadWrite, 65536, FileOptions.SequentialScan))
        //        using (var reader = new UInt16Reader(recordsStream))
        //        {
        //            // Keep reading while there's more data
        //            ushort[] records = null;
        //            while ((records = reader.ReadUInt16s()).Length != 0)
        //            {
        //                // Read as many values as we can from the reader's buffer
        //                for (var i = 0; i < records.Length; i++)
        //                {
        //                    var typeId = records[i];
        //                    var typeReader = typeReaders[typeId];

        //                    if (typeReader == null)
        //                        continue;

        //                    var handler = typeReader.Handler;
        //                    var instance = typeReader.ReadPooled();

        //                    handler(instance);
        //                }
        //            }
        //        }
        //    }

        //    foreach (var type in typeReaders)
        //    {
        //        type?.Dispose();
        //    }
        //}

        /// <summary>
        /// Collection of type readers, ordered by reader responsible for next message of interest (supports merge join).
        /// </summary>
        private class ReaderProgressSinglyLinkedList
        {
            private ReaderProgress _head;

            public ReaderProgressSinglyLinkedList(IEnumerable<TypeReaderFactory> typeReaders)
            {
                var progressRecords = typeReaders.Select(r =>
                    new ReaderProgress
                    {
                        NextSequenceId = r.ReadNextSequenceId() ?? UInt32.MaxValue, // TODO: Remove if nothing left
                        Reader = r
                    }).OrderBy(s => s.NextSequenceId);

                ReaderProgress last = null;
                foreach (var item in progressRecords)
                {
                    if (last == null)
                    {
                        _head = item;
                    }
                    else
                    {
                        AddAfter(last, item);
                    }

                    last = item;
                }
            }

            public ReaderProgress First => _head;

            public void AddAfter(ReaderProgress lastNode, ReaderProgress newNode)
            {
                newNode.Next = lastNode.Next;
                lastNode.Next = newNode;
            }

            public void RemoveFirst()
            {
                _head = _head.Next;
            }
        }

    }



    public sealed class SinglyLinkedListNode<T>
    {
        public SinglyLinkedListNode(T value)
        {
            Value = value;
        }

        public SinglyLinkedListNode<T> Next { get; internal set; }

        public T Value { get; set; }
    }
}