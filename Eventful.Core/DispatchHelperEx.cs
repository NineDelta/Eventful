using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Eventful.Core.Readers;

namespace Eventful.Core
{
	public static class EventProjectionEx
	{
        private class HandlerInvokers<T>
        {
            public Dictionary<Type, Action<T, object>> Handlers { get; set; }
        }

        private static readonly ConcurrentDictionary<Type, object> HandlerInvokerCache = new ConcurrentDictionary<Type, object>();


        public static T Into<T>(this IEventfulReader reader)
            where T : new()
        {
            return reader.Into(new T());
        }

        public static T Into<T>(this IEventfulReader reader, T projection)
        {
            var projectionSpec = HandlerInvokerCache.GetOrAdd(typeof(T), _ =>
            {
                return new HandlerInvokers<T>
                {
                    Handlers = typeof(T).GetMethods(BindingFlags.Public | BindingFlags.Instance)
                        .Where(r => r.Name == "Handle")
                        .ToDictionary(r => r.GetParameters().Single().ParameterType, r =>
                        {
                            var paramReadModel = Expression.Parameter(typeof(T), "r");
                            var paramArg = Expression.Parameter(typeof(object), "o");

                            var exprArg0 = Expression.Convert(paramArg, r.GetParameters().Single().ParameterType);
                            var exprCall = Expression.Call(paramReadModel, r, exprArg0);

                            var handlerDelegate = Expression
                                .Lambda<Action<T, object>>(exprCall, new[] {paramReadModel, paramArg})
                                .Compile();

                            return handlerDelegate;
                        })
                };
            });

            var handlers = ((HandlerInvokers<T>) projectionSpec).Handlers.ToDictionary(kvp => kvp.Key,
                kvp => (Action<object>) ((o) => kvp.Value(projection, o)));

            reader.Invoke(handlers);

            // Alternatively: Simpler API on event reader, but about 5% slower
            //var events = reader.Read(handlers.Keys.ToArray());

            //foreach (var e in events)
            //{
            //    // BaseType because of dynamic proxy
            //    handlers[e.GetType().BaseType](e);
            //}

            return projection;
        }
    }
}