using System;
using System.IO;

namespace Eventful.Core.Readers.DataTypes
{
    // Reader which leverages BinaryWriter for serialisation and deserialisation.
    // This is slow, so the goal is to replace this with data type specific buffered readers.

    public interface IPrimitiveReader : IDisposable
    {
        bool ReadBool();
    }

    /// <summary>
    /// We only want to create readers for properties which are actually called by user code.
    /// Adding an existence check branch in the getter body is slow, so instead we invoke a bootstrapper implementation which
    /// Instantiates the real implementation, then swaps out the reference within the proxy for the actual reader.
    /// 
    /// Note that the virtual method invocation on IDecimalReader is slower than invoking a non-virtual invocation.
    /// Thus, the next step is to have the proxy itself create the reader in the getter body, then rewrite the getter body to remove the instantiation on subsequent calls.
    /// </summary>
    public class PrimitiveBootstrapper : IPrimitiveReader
    {
        private readonly Func<Stream> _streamProvider;
        private readonly Action<object> _patcher;

        private PrimitiveReader _reader;

        public PrimitiveBootstrapper(Func<Stream> streamProvider, Action<object> patcher)
        {
            _patcher = patcher;
            _streamProvider = streamProvider;
        }

        public bool ReadBool()
        {
            _reader = new PrimitiveReader(_streamProvider());
            _patcher(_reader);
            return _reader.ReadBool();
        }

        public void Dispose()
        {
            _reader?.Dispose();
        }
    }

    public class PrimitiveReader : IPrimitiveReader
    {
        private readonly BinaryReader _binaryReader;

        public PrimitiveReader(Stream stream)
        {
            _binaryReader = new BinaryReader(stream);
        }

        public bool ReadBool()
        {
            return _binaryReader.ReadBoolean();
        }

        public void Dispose()
        {
            _binaryReader.Dispose();
            _binaryReader.BaseStream.Dispose();
        }
    }
}