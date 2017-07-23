using System;
using System.IO;
using System.Runtime.InteropServices;

namespace Eventful.Core.Readers.DataTypes
{
    public interface IDecimalReader : IDisposable
    {
        decimal ReadDecimal(long rowId);
    }

    /// <summary>
    /// We only want to create readers for properties which are actually called by user code.
    /// Adding an existence check branch in the getter body is slow, so instead we invoke a bootstrapper implementation which
    /// Instantiates the real implementation, then swaps out the reference within the proxy for the actual reader.
    /// 
    /// Note that the virtual method invocation on IDecimalReader is slower than invoking a non-virtual invocation.
    /// Thus, the next step is to have the proxy itself create the reader in the getter body, then rewrite the getter body to remove the instantiation on subsequent calls.
    /// </summary>
    public class DecimalBootstrapper : IDecimalReader
    {
        private readonly Func<Stream> _streamProvider;
        private readonly Action<object> _patcher;

        private DecimalReader _decimalReader;

        public DecimalBootstrapper(Func<Stream> streamProvider, Action<object> patcher)
        {
            _patcher = patcher;
            _streamProvider = streamProvider;
        }

        public decimal ReadDecimal(long rowId)
        {
            _decimalReader = new DecimalReader(_streamProvider());
            _patcher(_decimalReader);
            return _decimalReader.ReadDecimal(rowId);
        }

        public void Dispose()
        {
            _decimalReader?.Dispose();
        }
    }

    public class DecimalReader : IDecimalReader
    {
		private const int bufferSize = 65536;
		private readonly Stream stream;
		private readonly byte[] byteBuffer;
		private readonly decimal[] decimalBuffer;
		private int bufferOffset = 0;

		public DecimalReader(Stream stream)
		{
			this.stream = stream;
			byteBuffer = new byte[bufferSize];
			decimalBuffer = new decimal[bufferSize/16];

			ReadDecimals();
		}

		private void ReadDecimals()
		{
			var numBytesRead = stream.Read(byteBuffer, 0, bufferSize);

			var handle = GCHandle.Alloc(decimalBuffer, GCHandleType.Pinned);

			try
			{
				Marshal.Copy(byteBuffer, 0, handle.AddrOfPinnedObject(), numBytesRead);
			}
			finally
			{
				if (handle.IsAllocated)
					handle.Free();
			}
		}

        private long lastRowId = 0;

		public decimal ReadDecimal(long rowId)
		{
            // TODO: Support skipping multiple
		    if (rowId > lastRowId)
		    {
		        bufferOffset++;
		        lastRowId = rowId;
		    }

			if (bufferOffset == decimalBuffer.Length)
			{
				ReadDecimals();
				bufferOffset = 0;
			}

			var d = decimalBuffer[bufferOffset];
			return d;
		}
		
		public void Dispose()
		{
			stream.Close();
            stream.Dispose();
		}
	}
}