using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using Eventful.Core.Readers.DataTypes;

namespace Eventful.Core.Readers.ProxyGeneration
{
    /// <summary>
    /// TypeReader issues event proxy instances (pooled) to be dispatched to handlers.
    /// It also tracks the bookmark for the next event of the same type, so that different
    /// types (and TypeReaders) can be interleaved in correct global ordering.
    /// </summary>
    public class TypeReaderFactory : IDisposable
    {
        private readonly List<IDisposable> _columns = new List<IDisposable>();
        private readonly Func<object> _instanceFactory;
        private readonly object _instanceReusable;

        private readonly Lazy<UInt32Reader> sequenceReader;
        private string _folder;
        private Dictionary<Type, Func<BinaryReader, object>> _customReaders;
        public long rowId = -1;  // TODO: Move into proxy
        public Action<object> Handler;

        public TypeReaderFactory(string folder, Type type, Dictionary<Type, Func<BinaryReader, object>> customReaders)
        {
            _customReaders = customReaders;
            _folder = folder;
            var sequenceStream = new FileStream(folder + "sequence.log", FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite);
            sequenceReader = new Lazy<UInt32Reader>(() => new UInt32Reader(sequenceStream));

            var reusableProxyType = CreateProxyType(type);
            var reusableProxyInstance = Activator.CreateInstance(reusableProxyType);

            reusableProxyInstance.GetType().GetField("typeReader").SetValue(reusableProxyInstance, this);

            foreach (var property in RelevantProperties(reusableProxyType))
            {
                //var columnReader = new ColumnReader(folder, properties.Single(), customReaders);
                //var actualReader = columnReader.InnerReader;

                // TODO: Slight performance improvement from passing pointer to raw buffer array? Have a pool of proxyInstances and cycle between those, each
                // pointing to fixed spot in buffers. Rotate proxies and buffer at same time.

                var field = reusableProxyInstance.GetType().GetField(property.Name.ToLower());

                var placeholder = CreateReader(property.PropertyType, () => MiscReader.CreateStream(_folder, property), r => field.SetValue(reusableProxyInstance, r));
                
                field.SetValue(reusableProxyInstance, placeholder);

                _columns.Add(placeholder);
            }

            _instanceReusable = reusableProxyInstance;
            //var rowField = reusableProxyInstance.GetType().GetField("row");
            //_rowSetter = r => rowField.SetValue(reusableProxyInstance, r);
            _instanceFactory = Expression.Lambda<Func<object>>(Expression.New(type)).Compile();
        }

        private IDisposable CreateReader(Type propertyType, Func<Stream> stream, Action<object> proxyPatcher)
        {
            if (propertyType == typeof(bool))
            {
                return new PrimitiveBootstrapper(stream, proxyPatcher);
            }
            if (propertyType == typeof(decimal))
            {
                return new DecimalBootstrapper(stream, proxyPatcher);
            }

            return null;
        }

        public object ReadNew()
        {
            var instance = _instanceFactory();

            //for (var j = 0; j < _columns.Count; j++)
            //{
            //    _columns[j].Read(instance);
            //}

            return instance;
        }

        public object ReadPooled()
        {
            rowId++;
            //_rowSetter(rowId);
            return _instanceReusable;
            //var instance = _instanceReusable;

            //for (var j = 0; j < _columns.Length; j++)
            //{
            //	_columns[j].Read(instance);
            //}

            //return instance;
        }

        public uint? ReadNextSequenceId()
        {
            try
            {
                return sequenceReader.Value.ReadUInt32();
            }
            catch (EndOfStreamException)
            {
                return null;
            }
        }

        public uint ReadCount()
        {
            return sequenceReader.Value.ReadLength();
        }

        public void Dispose()
        {
            if (sequenceReader.IsValueCreated)
                sequenceReader.Value.Dispose();

            foreach (var column in _columns)
            {
                column.Dispose();
            }
        }

        private PropertyInfo[] RelevantProperties(Type type)
        {
            return type
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .Where(p => p.CanRead && p.CanWrite)
                .Where(p => p.GetMethod.IsVirtual && !p.GetMethod.IsFinal)
                .Where(p => p.PropertyType == typeof(decimal) || p.PropertyType == typeof(bool))
                .ToArray();
        }

        private static readonly Dictionary<Type, Type> _proxyTypeCache = new Dictionary<Type, Type>();
        //private Action<long> _rowSetter;

        private Type CreateProxyType(Type type)
        {
            if (_proxyTypeCache.ContainsKey(type))
                return _proxyTypeCache[type];

            var assName = new AssemblyName("Eventful.DynamicProxies");
            var assBuilder = AssemblyBuilder.DefineDynamicAssembly(assName, AssemblyBuilderAccess.Run);
            var moduleBuilder = assBuilder.DefineDynamicModule("testModule");
            var typeBuilder = moduleBuilder.DefineType(type.Name + "Proxy", TypeAttributes.Public);

            // Inherit from contract type
            typeBuilder.SetParent(type); //.AddInterfaceImplementation(typeOfT);

            
            var typeReaderField = typeBuilder.DefineField("typeReader", typeof(TypeReaderFactory), FieldAttributes.Public);
            var rowField = typeof(TypeReaderFactory).GetField("rowId");


            var properties = RelevantProperties(type);

            foreach (var item in properties)
            {
                // TODO: non-type specific here, perhaps just differentiate between "Read()" call and direct buffer access.
                if (item.PropertyType == typeof(bool))
                {
                    RegisterBoolProperty(typeBuilder, item);
                }
                else if (item.PropertyType == typeof(decimal))
                {
                    RegisterDecimalProperty(typeReaderField, rowField, typeBuilder, item);
                }
            }

            var constructedType = typeBuilder.CreateTypeInfo();

            _proxyTypeCache.Add(type, constructedType);

            return constructedType;
        }

        private void RegisterDecimalProperty(FieldBuilder typeReaderField, FieldInfo rowField, TypeBuilder typeBuilder, PropertyInfo property)
        {
            var readMethod = typeof(IDecimalReader).GetMethod("ReadDecimal");

            var fieldBuilder = typeBuilder.DefineField(property.Name.ToLower(), typeof(IDecimalReader), FieldAttributes.Public);

            var baseMethod = property.GetGetMethod();
            var getAccessor = typeBuilder.DefineMethod(baseMethod.Name, baseMethod.Attributes, property.PropertyType, null);
            var il = getAccessor.GetILGenerator();
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, fieldBuilder);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, typeReaderField);
            il.Emit(OpCodes.Ldfld, rowField);
            il.EmitCall(OpCodes.Call, readMethod, null);
            il.Emit(OpCodes.Ret);
            typeBuilder.DefineMethodOverride(getAccessor, baseMethod);
        }

        private void RegisterBoolProperty(TypeBuilder typeBuilder, PropertyInfo property)
        {
            var readMethod = typeof(IPrimitiveReader).GetMethod("ReadBool");

            var fieldBuilder = typeBuilder.DefineField(property.Name.ToLower(), typeof(IPrimitiveReader), FieldAttributes.Public);

            var baseMethod = property.GetGetMethod();
            var getAccessor = typeBuilder.DefineMethod(baseMethod.Name, baseMethod.Attributes, property.PropertyType, null);
            var il = getAccessor.GetILGenerator();
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, fieldBuilder);
            il.EmitCall(OpCodes.Call, readMethod, null);
            il.Emit(OpCodes.Ret);
            typeBuilder.DefineMethodOverride(getAccessor, baseMethod);
        }
    }
}