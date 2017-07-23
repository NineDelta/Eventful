using System;
using System.IO;

namespace Eventful.Core.Readers.DataTypes
{
	public class UInt32Reader : IDisposable
	{
		private const int bufferSize = 65536;
		private readonly Stream stream;
		private readonly byte[] byteBuffer;
		private uint[] uint32Buffer;

		public UInt32Reader(Stream stream)
		{
			this.stream = stream;
			byteBuffer = new byte[bufferSize];
			uint32Buffer = new uint[bufferSize / 4];
			ReadUInt32s();
		}

		private int bufferPosition = 0;

		public uint ReadLength()
		{
			return (uint) (stream.Length / 4);
		}

		public UInt32 ReadUInt32()
		{
			if (bufferPosition == uint32Buffer.Length)
			{
				ReadUInt32s();
				bufferPosition = 0;
			}

			var u = uint32Buffer[bufferPosition];
			bufferPosition++;
			return u;
		}

		private void ReadUInt32s()
		{
			var numBytesRead = stream.Read(byteBuffer, 0, bufferSize);

			if (numBytesRead == bufferSize)
			{
				Buffer.BlockCopy(byteBuffer, 0, uint32Buffer, 0, bufferSize);
			}
			else if (numBytesRead == 0)
			{
				throw new EndOfStreamException();
			}
			else
			{
				var partialUInt32Buffer = new uint[numBytesRead / 4];

				Buffer.BlockCopy(byteBuffer, 0, partialUInt32Buffer, 0, numBytesRead);

				uint32Buffer = partialUInt32Buffer;
			}
		}

		public void Dispose()
		{
			stream.Close();
		}
	}
}