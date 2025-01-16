﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Utils;
using Jint;
using Jint.Native;
using Lucene.Net.Documents;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Logging;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Rachis.Commands;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Sparrow.Server.Logging;
using Spatial4n.Shapes;

namespace Raven.Server.Documents.Indexes.Static
{
    public delegate IEnumerable IndexingFunc(IEnumerable<dynamic> items);

    public abstract class StaticCountersIndexBase : StaticCountersAndTimeSeriesIndexBase
    {
    }

    public abstract class StaticTimeSeriesIndexBase : StaticCountersAndTimeSeriesIndexBase
    {
    }

    public abstract class StaticCountersAndTimeSeriesIndexBase : AbstractStaticIndexBase
    {
        public void AddMap(string collection, string name, IndexingFunc map)
        {
            AddMapInternal(collection, name, map);
        }
    }

    public abstract class StaticIndexBase : AbstractStaticIndexBase
    {
        public void AddMap(string collection, IndexingFunc map)
        {
            AddMapInternal(collection, collection, map);
        }

        public dynamic LoadAttachments(dynamic doc)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException($"Indexing scope was not initialized.");

            if (doc is DynamicNullObject)
                return DynamicNullObject.Null;

            var document = doc as DynamicBlittableJson;
            if (document == null)
                throw new InvalidOperationException($"{nameof(LoadAttachments)} may only be called with a non-null entity as a parameter, but was called with a parameter of type {doc.GetType().FullName}: {doc}");

            return CurrentIndexingScope.Current.LoadAttachments(document);
        }

        public dynamic LoadAttachment(dynamic doc, object attachmentName)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException($"Indexing scope was not initialized. Attachment Name: {attachmentName}");

            if (doc is DynamicNullObject)
                return DynamicNullObject.Null;

            var document = doc as DynamicBlittableJson;
            if (document == null)
                throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with a non-null entity as a first parameter, but was called with a parameter of type {doc.GetType().FullName}: {doc}");

            if (attachmentName is LazyStringValue attachmentNameLazy)
                return CurrentIndexingScope.Current.LoadAttachment(document, attachmentNameLazy);

            if (attachmentName is string attachmentNameString)
                return CurrentIndexingScope.Current.LoadAttachment(document, attachmentNameString);

            if (attachmentName is DynamicNullObject)
                return DynamicNullObject.Null;

            throw new InvalidOperationException($"{nameof(LoadAttachment)} may only be called with a string, but was called with a parameter of type {attachmentName.GetType().FullName}: {attachmentName}");
        }

        public dynamic Id(dynamic doc)
        {
            if (doc is DynamicBlittableJson json)
                return json.GetId();

            if (doc is DynamicNullObject)
                return doc;

            ThrowInvalidDocType(doc, nameof(Id));

            // never hit
            return null;
        }

        public dynamic MetadataFor(dynamic doc)
        {
            if (doc is DynamicBlittableJson json)
            {
                json.EnsureMetadata();
                return doc[Constants.Documents.Metadata.Key];
            }

            if (doc is DynamicNullObject)
                return doc;

            ThrowInvalidDocType(doc, nameof(MetadataFor));

            // never hit
            return null;
        }

        public dynamic AsJson(dynamic doc)
        {
            if (doc is DynamicBlittableJson json)
            {
                json.EnsureMetadata();
                return json;
            }

            if (doc is DynamicNullObject)
                return doc;

            ThrowInvalidDocType(doc, nameof(AsJson));

            // never hit
            return null;
        }
        
        public dynamic AttachmentsFor(dynamic doc)
        {
            var metadata = MetadataFor(doc);
            var attachments = metadata is DynamicNullObject
                ? null : metadata[Constants.Documents.Metadata.Attachments];

            return attachments != null
                ? attachments
                : new DynamicArray(Enumerable.Empty<object>());
        }

        public dynamic CounterNamesFor(dynamic doc)
        {
            var metadata = MetadataFor(doc);
            var counters = metadata is DynamicNullObject
                ? null : metadata[Constants.Documents.Metadata.Counters];

            return counters != null
                ? counters
                : new DynamicArray(Enumerable.Empty<object>());
        }

        public dynamic TimeSeriesNamesFor(dynamic doc)
        {
            var metadata = MetadataFor(doc);
            var timeSeries = metadata is DynamicNullObject
                ? null : metadata[Constants.Documents.Metadata.TimeSeries];

            return timeSeries != null
                ? timeSeries
                : new DynamicArray(Enumerable.Empty<object>());
        }

        [DoesNotReturn]
        private static void ThrowInvalidDocType(dynamic doc, string funcName)
        {
            throw new InvalidOperationException(
                $"{funcName} may only be called with a document, " +
                $"but was called with a parameter of type {doc?.GetType().FullName}: {doc}");
        }
    }

    public abstract class AbstractStaticIndexBase
    {
        protected readonly Dictionary<string, CollectionName> _collectionsCache = new Dictionary<string, CollectionName>(StringComparer.OrdinalIgnoreCase);

        public readonly Dictionary<string, Dictionary<string, List<IndexingFunc>>> Maps = new Dictionary<string, Dictionary<string, List<IndexingFunc>>>();

        public readonly Dictionary<string, HashSet<CollectionName>> ReferencedCollections = new Dictionary<string, HashSet<CollectionName>>();

        public readonly HashSet<string> CollectionsWithCompareExchangeReferences = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        protected static RavenLogger Log = RavenLogManager.Instance.GetLoggerForServer<AbstractStaticIndexBase>();


        public int StackSizeInSelectClause { get; set; }
        
        public bool HasDynamicFields { get; set; }

        public bool HasBoostedFields { get; set; }
        
        public bool HasVectorFields { get; set; }

        public string Source;

        public IndexingFunc Reduce;

        public string[] OutputFields;

        public CompiledIndexField[] GroupByFields;

        private List<string> _groupByFieldNames;

        public List<string> GroupByFieldNames
        {
            get
            {
                return _groupByFieldNames ??= GroupByFields.Select(x => x.Name).ToList();
            }
        }

        public void AddCompareExchangeReferenceToCollection(string collection)
        {
            if (collection is null)
                throw new ArgumentNullException(nameof(collection));

            CollectionsWithCompareExchangeReferences.Add(collection);
        }

        public void AddReferencedCollection(string collection, string referencedCollection)
        {
            if (_collectionsCache.TryGetValue(referencedCollection, out CollectionName referencedCollectionName) == false)
                _collectionsCache[referencedCollection] = referencedCollectionName = new CollectionName(referencedCollection);

            if (ReferencedCollections.TryGetValue(collection, out HashSet<CollectionName> set) == false)
                ReferencedCollections[collection] = set = new HashSet<CollectionName>();

            set.Add(referencedCollectionName);
        }

        protected void AddMapInternal(string collection, string subCollecction, IndexingFunc map)
        {
            if (Maps.TryGetValue(collection, out Dictionary<string, List<IndexingFunc>> collections) == false)
                Maps[collection] = collections = new Dictionary<string, List<IndexingFunc>>();

            if (collections.TryGetValue(subCollecction, out var funcs) == false)
                collections[subCollecction] = funcs = new List<IndexingFunc>();

            funcs.Add(map);
        }

        internal void CheckDepthOfStackInOutputMap(IndexDefinition indexMetadata, DocumentDatabase documentDatabase)
        {
            var performanceHintConfig = documentDatabase.Configuration.PerformanceHints;
            if (StackSizeInSelectClause > performanceHintConfig.MaxDepthOfRecursionInLinqSelect)
            {
                documentDatabase.NotificationCenter.Add(PerformanceHint.Create(
                    documentDatabase.Name,
                    $"Index '{indexMetadata.Name}' contains {StackSizeInSelectClause} `let` clauses.",
                    $"We have detected that your index contains many `let` clauses. This can be not optimal approach because it might cause to allocate a lot of stack-based memory. Please consider to simplify your index definition. We suggest not to exceed {performanceHintConfig.MaxDepthOfRecursionInLinqSelect} `let` statements.",
                    PerformanceHintType.Indexing,
                    NotificationSeverity.Info,
                    nameof(IndexCompiler)));
                
                if (Log.IsWarnEnabled)
                    Log.Warn($"Index '{indexMetadata.Name}' contains a lot of `let` clauses. Stack size is {StackSizeInSelectClause}.");
            }
        }
        
        protected dynamic TryConvert<T>(object value)
            where T : struct
        {
            if (value == null || value is DynamicNullObject)
                return DynamicNullObject.Null;

            var type = typeof(T);
            if (type == typeof(double) || type == typeof(float))
            {
                var dbl = TryConvertToDouble(value);
                if (dbl.HasValue == false)
                    return DynamicNullObject.Null;

                if (type == typeof(float))
                    return (T)(object)Convert.ToSingle(dbl.Value);

                return (T)(object)dbl.Value;
            }

            if (type == typeof(long) || type == typeof(int))
            {
                var lng = TryConvertToLong(value);
                if (lng.HasValue == false)
                    return DynamicNullObject.Null;

                if (type == typeof(int))
                    return (T)(object)Convert.ToInt32(lng.Value);

                return (T)(object)lng.Value;
            }

            return DynamicNullObject.Null;

            static double? TryConvertToDouble(object v)
            {
                if (v is double d)
                    return d;
                if (v is LazyNumberValue lnv)
                    return lnv;
                if (v is int i)
                    return i;
                if (v is long l)
                    return l;
                if (v is float f)
                    return f;
                if (v is LazyStringValue lsv && double.TryParse(lsv, out var r))
                    return r;

                return null;
            }

            static long? TryConvertToLong(object v)
            {
                if (v is double d)
                    return (long)d;
                if (v is LazyNumberValue lnv)
                    return lnv;
                if (v is int i)
                    return i;
                if (v is long l)
                    return l;
                if (v is float f)
                    return (long)f;
                if (v is LazyStringValue lsv && long.TryParse(lsv, out var r))
                    return r;

                return null;
            }
        }

        /// <summary>
        /// Dictionary training process occurs in IsOnBeforeExecuteIndexing. Since we're not training dictionaries with vectors, and considering computation
        /// power required (e.g., generating embeddings from text), it is better to skip that part as there is no benefit in performing it.
        /// </summary>
        /// <param name="currentIndexingScope">Current indexing scope.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsDictionaryTrainingPhase(CurrentIndexingScope currentIndexingScope)
        {
            return currentIndexingScope != null && currentIndexingScope.Index.IsOnBeforeExecuteIndexing;
        }

        internal static IndexField RetrieveVectorField(string fieldName, object value)
        {
            var currentIndexingScope = CurrentIndexingScope.Current;
            var fieldExists = currentIndexingScope.Index.Definition.IndexFields.TryGetValue(fieldName, out var indexField);
            if (fieldExists == false || indexField?.Vector is null)
            {
                // We're supporting two defaults:
                // when Options are not set, we'll decide what is configuration in following manner:
                // - value is textual or array of textual we're treating them as text input
                // - otherwise, we will write as array of numerical values
                var isText = IsText(value);
                if (isText == false && value is IEnumerable enumerable)
                {
                    foreach (var item in enumerable)
                    {
                        if (item is null or DynamicNullObject)
                            continue;
                        
                        isText = item is LazyStringValue or LazyCompressedStringValue or string or DynamicNullObject or JsString;
                        break;
                    }
                }
                
                indexField = currentIndexingScope.GetOrCreateVectorField(fieldName, isText);
            }

            PortableExceptions.ThrowIf<InvalidDataException>(indexField?.Vector is null,
                $"Field '{fieldName}' does not exist in this indexing scope. Cannot index as vector.");

            indexField!.Vector!.ValidateDebug();

            return indexField;
            
            bool IsText(object item)
            {
                return item is LazyStringValue or LazyCompressedStringValue or string or DynamicNullObject or JsString;
            }
        }
        
        public object CreateVector(string fieldName, object value)
        {
            if (value is null or DynamicNullObject)
                return null;

            var currentIndexingScope = CurrentIndexingScope.Current;
            if (IsDictionaryTrainingPhase(currentIndexingScope))
                return null;
            

            var indexField = RetrieveVectorField(fieldName, value);
            var vector = indexField!.Vector!.SourceEmbeddingType switch
            { 
                VectorEmbeddingType.Text => VectorFromText(indexField, value),
                _ => VectorFromEmbedding(indexField, value)
            };
            
            if (indexField.Id != Corax.Constants.IndexWriter.DynamicField)
                return vector;
            
            currentIndexingScope.DynamicFields ??= new Dictionary<string, IndexField>();
            if (currentIndexingScope.DynamicFields.TryGetValue(fieldName, out var existing) == false)
            {
                currentIndexingScope.DynamicFields[fieldName] = indexField;
                currentIndexingScope.IncrementDynamicFields();
            }

            return new CoraxDynamicItem() { Field = indexField, Value = vector };
        }

        /// <summary>
        /// Create vector field object. This method is used by AutoIndexes and JavaScript indexes.
        /// </summary>
        /// <param name="indexField">IndexField from IndexDefinition</param>
        /// <param name="value">Data source to create vector field.</param>
        /// <returns></returns>
        internal static object CreateVector(IndexField indexField, object value, bool isAutoIndex)
        {
            if (IsDictionaryTrainingPhase(CurrentIndexingScope.Current))
                return null;
            
            return indexField!.Vector!.SourceEmbeddingType switch
            {
                VectorEmbeddingType.Text => VectorFromText(indexField, value),
                _ => VectorFromEmbedding(indexField, value, isAutoIndex)
            };
        }

        private static object VectorFromEmbedding(IndexField currentIndexingField, object value, bool isAutoIndex = false)
        {
            if (value is null)
                return VectorValue.Null;
            
            var vectorOptions = currentIndexingField.Vector;
            var allocator = CurrentIndexingScope.Current.IndexContext.Allocator;
            if (value is LazyStringValue or LazyCompressedStringValue or string or DynamicNullObject)
                return Base64ToVector(value);

            if (value is BlittableJsonReaderArray || value is DynamicArray { Inner: BlittableJsonReaderArray })
            {
                var bjra = value as BlittableJsonReaderArray ?? (BlittableJsonReaderArray)((DynamicArray)value).Inner;
                return HandleBlittableJsonReaderArray(bjra);
            }

            if (value is BlittableJsonReaderObject or DynamicBlittableJson)
            {
                var bjro = value as BlittableJsonReaderObject ?? ((DynamicBlittableJson)value).BlittableJson;
                if (bjro.TryGetMember(Sparrow.Global.Constants.Naming.VectorPropertyName, out var vector) && vector is BlittableJsonReaderVector bjrv)
                {
                    return HandleBlittableJsonReaderVector(bjrv);
                }

                throw new ArgumentException($"Expected BlittableJsonReaderVector, but got {value.GetType().FullName}");
            }

            if (value is Stream stream)
                return HandleStream(stream);
            
            if (value is JsArray js)
                return HandleJsArray(js);

            if (value is IEnumerable ie)
                return HandleEnumerable(ie);
            
            throw new InvalidOperationException($"Unknown type. Value type: {value.GetType().FullName}");

            object Base64ToVector(object base64)
            {
                var str = base64.ToString();
                return GenerateEmbeddings.FromBase64Array(vectorOptions, allocator, str, isAutoIndex);
            }

            object HandleEnumerable(IEnumerable enumerable)
            {
                var enumerator = enumerable.GetEnumerator();
                
                using var _ = enumerator as IDisposable;
                if (enumerator.MoveNext() == false)
                    return null;

                var isBase = IsBase64(enumerator.Current);
                var isStream = enumerator.Current is Stream;
                List<object> values = new();
                
                do
                {
                    ProcessItem(enumerator.Current);
                }
                while (enumerator.MoveNext());
                
                return values;

                void ProcessItem(object item)
                {
                    if (isBase)
                    {
                        values.Add(Base64ToVector(item));
                        return;
                    }

                    if (isStream)
                    {
                        values.Add(HandleStream((Stream)item));
                        return;
                    }
                    
                    switch (vectorOptions.SourceEmbeddingType)
                    {
                        case VectorEmbeddingType.Single:
                        {
                            var memScope = allocator.Allocate((((float[])item)!).Length * sizeof(float), out Memory<byte> mem);
                            MemoryMarshal.Cast<float, byte>((float[])item).CopyTo(mem.Span);
                            var vector = GenerateEmbeddings.FromArray(allocator, memScope, mem, vectorOptions, mem.Length);
                            values.Add(vector);
                            return;
                        }
                        case VectorEmbeddingType.Int8:
                        {
                            var memScope = allocator.Allocate((((sbyte[])item)!).Length, out Memory<byte> mem);
                            MemoryMarshal.Cast<sbyte, byte>((sbyte[])item).CopyTo(mem.Span);
                            var vector = GenerateEmbeddings.FromArray(allocator, memScope, mem, vectorOptions, mem.Length);
                            values.Add(vector);
                            return;
                        }
                        default:
                        {
                            var memScope = allocator.Allocate((((byte[])item)!).Length, out Memory<byte> mem);
                            ((byte[])item).CopyTo(mem.Span);
                            var vector = GenerateEmbeddings.FromArray(allocator, memScope, mem, vectorOptions, mem.Length);
                            values.Add(vector);
                            break;
                        }
                    }
                }
            }
            
            object HandleBlittableJsonReaderArray(BlittableJsonReaderArray data)
            {
                var dataLength = data.Length;
                
                if (TryGetFirstNonNullElement(data, out var firstNonNull) == false)
                    return VectorValue.Null;
                
                //Array of base64s
                if (IsBase64(firstNonNull))
                {
                    var values = new object[dataLength];
                    for (var i = 0; i < dataLength; i++)
                        values[i] = Base64ToVector(data[i].ToString());

                    return values;
                }
                
                //Array of arrays
                if (firstNonNull is BlittableJsonReaderArray)
                {
                    var values = new object[dataLength];
                    for (var i = 0; i < dataLength; i++)
                    {
                        if (data[i] == null)
                            values[i] = VectorValue.Null;
                        else
                            values[i] = HandleBlittableJsonReaderArray((BlittableJsonReaderArray)data[i]);
                    }

                    return values;
                }
                
                var bufferSize = dataLength * (vectorOptions.SourceEmbeddingType) switch
                {
                    VectorEmbeddingType.Single => sizeof(float),
                    _ => sizeof(byte)
                };

                var memScope = allocator.Allocate(bufferSize, out Memory<byte> mem);
                ref var floatRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, float>(mem.Span));
                ref var sbyteRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, sbyte>(mem.Span));
                ref var byteRef = ref MemoryMarshal.GetReference(mem.Span);
                
                for (int i = 0; i < dataLength; ++i)
                {
                    switch (vectorOptions.SourceEmbeddingType)
                    {
                        case VectorEmbeddingType.Single:
                            Unsafe.Add(ref floatRef, i) = data.GetByIndex<float>(i);
                            break;
                        case VectorEmbeddingType.Int8:
                            Unsafe.Add(ref sbyteRef, i) = data.GetByIndex<sbyte>(i);
                            break;
                        default:
                            Unsafe.AddByteOffset(ref byteRef, i) = data.GetByIndex<byte>(i);
                            break;
                    }
                }

                return GenerateEmbeddings.FromArray(allocator, memScope, mem, vectorOptions, bufferSize);
            }

            object HandleBlittableJsonReaderVector(BlittableJsonReaderVector bjrv)
            {
                var type = bjrv[0].GetType();

                if (type == typeof(float))
                    return HandleBjrvInternal(bjrv.ReadArray<float>());
                if (type == typeof(double))
                    return HandleBjrvInternal(bjrv.ReadArray<double>());
                if (type == typeof(byte))
                    return HandleBjrvInternal(bjrv.ReadArray<byte>());
                if (type == typeof(sbyte))
                    return HandleBjrvInternal(bjrv.ReadArray<sbyte>());
                
                throw new NotSupportedException($"Embeddings of type {type.FullName} are not supported.");
            }

            object HandleBjrvInternal<T>(ReadOnlySpan<T> embedding) where T : unmanaged
            {
                var bufferSize = embedding.Length * (vectorOptions.SourceEmbeddingType) switch
                {
                    VectorEmbeddingType.Single => sizeof(float),
                    _ => sizeof(byte)
                };
                
                var memScope = allocator.Allocate(bufferSize, out Memory<byte> mem);
                
                MemoryMarshal.Cast<T, byte>(embedding).CopyTo(mem.Span);
                
                return GenerateEmbeddings.FromArray(allocator, memScope, mem, vectorOptions, bufferSize);
            }

            object HandleJsArray(JsArray jsArray)
            {
                var firstItem = jsArray[0];
                if (firstItem.IsString())
                {
                    var values = new object[jsArray.Length];
                    for (var i = 0; i < jsArray.Length; i++)
                        values[i] = GenerateEmbeddings.FromBase64Array(vectorOptions, allocator, jsArray[i].AsString());

                    return values;
                }

                if (firstItem is JsArray)
                {
                    var values = new object[jsArray.Length];
                    for (var i = 0; i < jsArray.Length; i++)
                        values[i] = HandleJsArray(jsArray[i] as JsArray);

                    return values;
                }

                var len = (int)jsArray.Length;
                var bufferSize = len * (vectorOptions.SourceEmbeddingType) switch
                {
                    VectorEmbeddingType.Single => sizeof(float),
                    _ => sizeof(byte)
                };

                var memScope = allocator.Allocate(bufferSize, out Memory<byte> mem);
                ref var floatRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, float>(mem.Span));
                ref var sbyteRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, sbyte>(mem.Span));
                ref var byteRef = ref MemoryMarshal.GetReference(mem.Span);
                
                for (int i = 0; i < len; ++i)
                {
                    var num = jsArray[i].AsNumber();
                    switch (vectorOptions.SourceEmbeddingType)
                    {
                        case VectorEmbeddingType.Single:
                            Unsafe.Add(ref floatRef, i) = (float)num;
                            break;
                        case VectorEmbeddingType.Int8:
                            Unsafe.Add(ref sbyteRef, i) = Convert.ToSByte(num);
                            break;
                        default:
                            Unsafe.AddByteOffset(ref byteRef, i) = Convert.ToByte(num);
                            break;
                    }
                }
                
                return GenerateEmbeddings.FromArray(allocator, memScope, mem, vectorOptions, bufferSize);
            }
            
            object HandleStream(Stream stream)
            {
                var len = (int)stream.Length;
                var memScope = allocator.Allocate((int)stream.Length, out Memory<byte> mem);
                stream.ReadExactly(mem.Span);
                return GenerateEmbeddings.FromArray(allocator, memScope, mem, vectorOptions, len);
            }
            
            bool IsBase64(object val) => val is LazyStringValue or LazyCompressedStringValue or string or DynamicNullObject or JsString;
            
            bool TryGetFirstNonNullElement(BlittableJsonReaderArray data, out object first)
            {
                first = data[0];
                
                var i = 0;
                
                while (first is null && i < data.Length)
                    first = data[i++];
                
                if (first == null)
                    return false;

                return true;
            }
        }

        private static object VectorFromText(IndexField indexField, object value)
        {
            if (value is null)
                return null;

            var allocator = CurrentIndexingScope.Current.IndexContext.Allocator;
            object embedding = null;
            if (value is LazyStringValue or LazyCompressedStringValue or string or DynamicNullObject)
                embedding = CreateVectorValue(value);
            else if (value is IEnumerable enumerable)
            {
                var vectorList = new List<VectorValue>();
                foreach (var item in enumerable)
                {
                    vectorList.Add(CreateVectorValue(item));
                }
                
                embedding = vectorList;
            }

            PortableExceptions.ThrowIf<InvalidDataException>(embedding is null, $"Tried to convert text into embeddings but got type {value?.GetType().FullName} which is not supported.");
            return embedding;

            VectorValue CreateVectorValue(object valueToProcess)
            {
                var str = valueToProcess switch
                {
                    LazyStringValue lsv => (string)lsv,
                    LazyCompressedStringValue lcsv => lcsv,
                    string s => s,
                    DynamicNullObject => null,
                    null => null,
                    _ => throw new NotSupportedException("Only strings are supported, but got: " + valueToProcess.GetType().FullName)
                };

                if (str == null)
                    return VectorValue.Null;

                return GenerateEmbeddings.FromText(allocator, indexField.Vector, str);
            }
        }

        public dynamic LoadDocument<TIgnored>(object keyOrEnumerable, string collectionName)
        {
            return LoadDocument(keyOrEnumerable, collectionName);
        }

        public dynamic LoadDocument(object keyOrEnumerable, string collectionName)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException(
                    "Indexing scope was not initialized. Key: " + keyOrEnumerable);

            if (keyOrEnumerable is LazyStringValue keyLazy)
                return CurrentIndexingScope.Current.LoadDocument(keyLazy, null, collectionName);

            if (keyOrEnumerable is string keyString)
                return CurrentIndexingScope.Current.LoadDocument(null, keyString, collectionName);

            if (keyOrEnumerable is DynamicNullObject || keyOrEnumerable is null)
                return DynamicNullObject.Null;

            if (keyOrEnumerable is IEnumerable enumerable)
            {
                var enumerator = enumerable.GetEnumerator();
                using (enumerable as IDisposable)
                {
                    var items = new List<dynamic>();
                    while (enumerator.MoveNext())
                    {
                        items.Add(LoadDocument(enumerator.Current, collectionName));
                    }
                    if (items.Count == 0)
                        return DynamicNullObject.Null;

                    return new DynamicArray(items);
                }
            }

            throw new InvalidOperationException(
                "LoadDocument may only be called with a string or an enumerable, but was called with a parameter of type " +
                keyOrEnumerable.GetType().FullName + ": " + keyOrEnumerable);
        }

        public dynamic LoadCompareExchangeValue<TIgnored>(object keyOrEnumerable)
        {
            return LoadCompareExchangeValue(keyOrEnumerable);
        }

        public dynamic LoadCompareExchangeValue(object keyOrEnumerable)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException(
                    "Indexing scope was not initialized. Key: " + keyOrEnumerable);

            if (keyOrEnumerable is LazyStringValue keyLazy)
                return CurrentIndexingScope.Current.LoadCompareExchangeValue(keyLazy, null);

            if (keyOrEnumerable is string keyString)
                return CurrentIndexingScope.Current.LoadCompareExchangeValue(null, keyString);

            if (keyOrEnumerable is DynamicNullObject)
                return DynamicNullObject.Null;

            if (keyOrEnumerable is IEnumerable enumerable)
            {
                var enumerator = enumerable.GetEnumerator();
                using (enumerable as IDisposable)
                {
                    var items = new List<dynamic>();
                    while (enumerator.MoveNext())
                    {
                        items.Add(LoadCompareExchangeValue(enumerator.Current));
                    }
                    if (items.Count == 0)
                        return DynamicNullObject.Null;

                    return new DynamicArray(items);
                }
            }

            throw new InvalidOperationException(
                "LoadCompareExchangeValue may only be called with a string or an enumerable, but was called with a parameter of type " +
                keyOrEnumerable.GetType().FullName + ": " + keyOrEnumerable);
        }

        public IEnumerable<dynamic> Recurse(object item, Func<dynamic, dynamic> func)
        {
            return new RecursiveFunction(item, func).Execute();
        }

        protected IEnumerable<object> CreateField(string name, object value, CreateFieldOptions options)
        {
            if (CurrentIndexingScope.Current.SupportsDynamicFieldsCreation == false)
                return null;

            var scope = CurrentIndexingScope.Current;
            return scope.Index.SearchEngineType switch
            {
                SearchEngineType.Corax => CoraxCreateField(scope, name, value, options),
                _ => LuceneCreateField(scope, name, value, options)
            };
        }

        protected IEnumerable<CoraxDynamicItem> CoraxCreateField(CurrentIndexingScope scope, string name, object value, CreateFieldOptions options)
        {
            IndexFieldOptions allFields = null;
            if (scope.IndexDefinition is MapIndexDefinition mapIndexDefinition)
            {
                mapIndexDefinition.IndexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out allFields);
            }

            var field = IndexField.Create(name, new IndexFieldOptions
            {
                Storage = options?.Storage,
                TermVector = options?.TermVector,
                Indexing = options?.Indexing,
            }, allFields, Corax.Constants.IndexWriter.DynamicField);

            scope.DynamicFields ??= new Dictionary<string, IndexField>();
            if (scope.DynamicFields.TryGetValue(name, out var existing) == false)
            {
                scope.DynamicFields[name] = field;
                scope.IncrementDynamicFields();
            }
            else if (options?.Indexing != null && existing.Indexing != field.Indexing)
            {
                throw new InvalidDataException($"Inconsistent dynamic field creation options were detected. Field '{name}' was created with '{existing.Indexing}' analyzer but now '{field.Indexing}' analyzer was specified. This is not supported");
            }


            var result = new List<CoraxDynamicItem>
            {
                new()
                {
                    Field = field,
                    FieldName = name,
                    Value = value
                }
            };

            return result;
        }
        
        private IEnumerable<AbstractField> LuceneCreateField(CurrentIndexingScope scope, string name, object value, CreateFieldOptions options)
        {
            // IMPORTANT: Do not delete this method, it is used by the indexes code when using CreateField

            options = options ?? CreateFieldOptions.Default;

            IndexFieldOptions allFields = null;
            if (scope.IndexDefinition is MapIndexDefinition mapIndexDefinition)
                mapIndexDefinition.IndexDefinition.Fields.TryGetValue(Constants.Documents.Indexing.Fields.AllFields, out allFields);

            var field = IndexField.Create(name, new IndexFieldOptions
            {
                Storage = options.Storage,
                TermVector = options.TermVector,
                Indexing = options.Indexing
            }, allFields);

            if (scope.DynamicFields == null)
                scope.DynamicFields = new Dictionary<string, IndexField>();

            scope.DynamicFields[name] = field;

            if (scope.CreateFieldConverter == null)
                scope.CreateFieldConverter = new LuceneDocumentConverter(scope.Index, new IndexField[] { });

            using var i = scope.CreateFieldConverter.NestedField(scope.CreatedFieldsCount);
            scope.IncrementDynamicFields();
            var result = new List<AbstractField>();
            scope.CreateFieldConverter.GetRegularFields(new StaticIndexLuceneDocumentWrapper(result), field, value, CurrentIndexingScope.Current.IndexContext, scope?.Source, out _);
            return result;
        }
        
        protected IEnumerable<object> CreateField(string name, object value, bool stored = false, bool? analyzed = null)
        {
            if (CurrentIndexingScope.Current.SupportsDynamicFieldsCreation == false)
                return null;
            // IMPORTANT: Do not delete this method, it is used by the indexes code when using CreateField

            FieldIndexing? indexing;

            switch (analyzed)
            {
                case true:
                    indexing = FieldIndexing.Search;
                    break;
                case false:
                    indexing = FieldIndexing.Exact;
                    break;
                default:
                    indexing = null;
                    break;
            }

            var scope = CurrentIndexingScope.Current;
            var creationFieldOptions = new CreateFieldOptions
            {
                Storage = stored ? FieldStorage.Yes : FieldStorage.No, Indexing = indexing, TermVector = FieldTermVector.No
            };
            return scope.Index.SearchEngineType switch
            {
                SearchEngineType.Corax => CoraxCreateField(scope, name, value, creationFieldOptions),
                _ => LuceneCreateField(scope, name, value, creationFieldOptions)
            };
        }

        public unsafe dynamic AsDateOnly(dynamic field)
        {
            if (field is LazyStringValue lsv)
            {
                if (LazyStringParser.TryParseDateOnly(lsv.Buffer, lsv.Length, out var @do) == false) 
                    return DynamicNullObject.Null;
                
                return @do;
            }
            
            if (field is string str)
            {
                fixed (char* strBuffer = str.AsSpan())
                {
                    if (LazyStringParser.TryParseDateOnly(strBuffer, str.Length, out var to) == false)
                        return DynamicNullObject.Null;

                    return to;
                }
            }
            
            if (field is DateTime dt)
            {
                return DateOnly.FromDateTime(dt);
            }

            if (field is DateOnly dtO)
            {
                return dtO;
            }
            
            if (field is null)
            {
                return DynamicNullObject.ExplicitNull;
            }
            
            if (field is DynamicNullObject dno)
            {
                return dno;
            }
            
            throw new InvalidDataException($"Expected {nameof(DateTime)}, {nameof(DateOnly)}, null, string or JSON value.");
        }

        public unsafe dynamic AsTimeOnly(dynamic field)
        {
            if (field is LazyStringValue lsv)
            {
                if (LazyStringParser.TryParseTimeOnly(lsv.Buffer, lsv.Length, out var to) == false)
                    return DynamicNullObject.Null;

                return to;
            }

            if (field is string str)
            {
                fixed (char* strBuffer = str.AsSpan())
                {
                    if (LazyStringParser.TryParseTimeOnly(strBuffer, str.Length, out var to) == false)
                        return DynamicNullObject.Null;

                    return to;
                }
            }
            
            if (field is TimeSpan ts)
            {
                return TimeOnly.FromTimeSpan(ts);
            }

            if (field is TimeOnly toF)
            {
                return toF;
            }

            if (field is null)
            {
                return DynamicNullObject.ExplicitNull;
            }
            
            if (field is DynamicNullObject dno)
            {
                return dno;
            }
            
            throw new InvalidDataException($"Expected {nameof(TimeSpan)}, {nameof(TimeOnly)}, null, string or JSON value.");
        }

        public IEnumerable<object> CreateSpatialField(string name, object lat, object lng)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");
            
            return CreateSpatialField(name, ConvertToDouble(lat), ConvertToDouble(lng));
        }

        public IEnumerable<object> CreateSpatialField(string name, double? lat, double? lng)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");
            
            var spatialField = GetOrCreateSpatialField(name);

            return CreateSpatialField(spatialField, lat, lng);
        }

        public IEnumerable<object> CreateSpatialField(string name, object shapeWkt)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");
            
            var spatialField = GetOrCreateSpatialField(name);
            return CreateSpatialField(spatialField, shapeWkt);
        }

        internal static IEnumerable<object> CreateSpatialField(SpatialField spatialField, object lat, object lng)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");
            
            return CreateSpatialField(spatialField, ConvertToDouble(lat), ConvertToDouble(lng));
        }

        internal static IEnumerable<object> CreateSpatialField(SpatialField spatialField, double? lat, double? lng)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");

            if (CurrentIndexingScope.Current.SupportsSpatialFieldsCreation == false)
                return null;

            if (lng == null || double.IsNaN(lng.Value))
                return Enumerable.Empty<AbstractField>();
            if (lat == null || double.IsNaN(lat.Value))
                return Enumerable.Empty<AbstractField>();

            IShape shape = spatialField.GetContext().MakePoint(lng.Value, lat.Value);
            return CurrentIndexingScope.Current.Index.SearchEngineType is SearchEngineType.Lucene
                ? spatialField.LuceneCreateIndexableFields(shape)
                : spatialField.CoraxCreateIndexableFields(shape);
        }

        internal static IEnumerable<object> CreateSpatialField(SpatialField spatialField, object shapeWkt)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");

            if (CurrentIndexingScope.Current.SupportsSpatialFieldsCreation == false)
                return null;

            return CurrentIndexingScope.Current.Index.SearchEngineType is SearchEngineType.Lucene
                ? spatialField.LuceneCreateIndexableFields(shapeWkt)
                : spatialField.CoraxCreateIndexableFields(shapeWkt);
        }

        internal static SpatialField GetOrCreateSpatialField(string name)
        {
            if (CurrentIndexingScope.Current == null)
                throw new InvalidOperationException("Indexing scope was not initialized.");

            if (CurrentIndexingScope.Current.SupportsSpatialFieldsCreation == false)
                return null;

            return CurrentIndexingScope.Current.GetOrCreateSpatialField(name);
        }

        private static double? ConvertToDouble(object value)
        {
            if (value == null || value is DynamicNullObject)
                return null;

            if (value is LazyNumberValue lnv)
                return lnv.ToDouble(CultureInfo.InvariantCulture);

            return Convert.ToDouble(value);
        }

        internal struct StaticIndexLuceneDocumentWrapper : ILuceneDocumentWrapper
        {
            private readonly List<AbstractField> _fields;

            public StaticIndexLuceneDocumentWrapper(List<AbstractField> fields)
            {
                _fields = fields;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Add(AbstractField field)
            {
                _fields.Add(field);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public IList<IFieldable> GetFields()
            {
                throw new NotImplementedException();
            }
        }
    }
}
