using System.Collections.Generic;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.Test;

public class TestIndexRun
{
    private readonly JsonOperationContext _context;
    public List<BlittableJsonReaderObject> MapResults;
    public List<BlittableJsonReaderObject> ReduceResults;
    private Dictionary<string, bool> _collectionTracker;
    private readonly int _maxDocumentsPerIndex;

    public TestIndexRun(JsonOperationContext context, int maxDocumentsPerIndex)
    {
        _context = context;
        MapResults = new List<BlittableJsonReaderObject>();
        ReduceResults = new List<BlittableJsonReaderObject>();
        _maxDocumentsPerIndex = maxDocumentsPerIndex;
    }

    public TestIndexWriteOperation CreateIndexWriteOperationWrapper(IndexWriteOperationBase writer, Index index)
    {
        return new TestIndexWriteOperation(writer, index);
    }

    public IIndexedItemEnumerator CreateEnumeratorWrapper(IIndexedItemEnumerator enumerator, string collection, int collectionsCount)
    {
        _collectionTracker ??= new Dictionary<string, bool>();
        if (_collectionTracker.ContainsKey(collection))
            return new EmptyItemEnumerator();
                
        _collectionTracker[collection] = true;
        return new TestIndexItemEnumerator(enumerator, collectionsCount, _maxDocumentsPerIndex);
    }

    public void AddMapResult(object result)
    {
        var item = ConvertToBlittable(result);
            
        MapResults.Add(item);
    }

    public void AddMapResult(BlittableJsonReaderObject mapResult, string collection)
    {
        BlittableJsonReaderObject result = mapResult.Clone(_context);
            
        MapResults.Add(result);
    }
        
    public void AddReduceResult(object result)
    {
        var item = ConvertToBlittable(result);
            
        ReduceResults.Add(item);
    }
        
    private BlittableJsonReaderObject ConvertToBlittable(object result)
    {
        var djv = new DynamicJsonValue();
        IPropertyAccessor propertyAccessor = PropertyAccessor.Create(result.GetType(), result);

        foreach (var property in propertyAccessor.GetProperties(result))
        {
            var value = property.Value;
            djv[property.Key] = TypeConverter.ToBlittableSupportedType(value, context: _context);
        }
            
        return _context.ReadObject(djv, "test-index-result");
    }
}