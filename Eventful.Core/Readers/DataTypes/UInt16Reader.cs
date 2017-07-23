using System;
using System.IO;

namespace Eventful.Core.Readers.DataTypes
{
	public class UInt16Reader : IDisposable
	{
		private const int bufferSize = 65536;
		private readonly Stream stream;
		private readonly byte[] byteBuffer;
		private readonly ushort[] uint16Buffer;

		public UInt16Reader(Stream stream)
		{
			this.stream = stream;
			byteBuffer = new byte[bufferSize];
			uint16Buffer = new ushort[bufferSize / 2];
		}

		public UInt16[] ReadUInt16s()
		{
			var numBytesRead = stream.Read(byteBuffer, 0, bufferSize);

			if (numBytesRead == bufferSize)
			{
				Buffer.BlockCopy(byteBuffer, 0, uint16Buffer, 0, bufferSize);

				return uint16Buffer;
			}
			else if (numBytesRead == 0)
			{
				return new ushort[0];
			}
			else
			{
				var partialUint16Buffer = new ushort[numBytesRead / 2];

				Buffer.BlockCopy(byteBuffer, 0, partialUint16Buffer, 0, numBytesRead);

				return partialUint16Buffer;
			}
		}

		public void Dispose()
		{
			stream.Close();
		}
	}
}