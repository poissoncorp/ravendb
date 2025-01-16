﻿using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Vectors;

public class VectorIndexingWithMixedFields(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void CanIndexNullWithVector()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Store(new AutoVecDoc(string.Empty, new float[] { .1f, .2f }, null, null, 0f, 0f));
        session.Store(new AutoVecDoc(string.Empty, null, null, null, 0f, 0f));
        session.SaveChanges();

        var result = session.Query<AutoVecDoc>()
            .Customize(x => x.WaitForNonStaleResults())
            .VectorSearch(x => x.WithEmbedding(p => p.Singles), v => v.ByEmbedding([.1f, .2f]))
            .Single();
        Assert.NotNull(result.Singles);
        result = session.Advanced.RawQuery<AutoVecDoc>($"from index 'Auto/AutoVecDocs/ByVector.search(Singles)' where 'vector.search(Singles)' == null").Single();
        Assert.Null(result.Singles);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void CanIndexNullWhenServerGeneratesTheEmbedding()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Store(new AutoVecDoc("animal", null, null, null, 0f, 0f));
        session.Store(new AutoVecDoc(null, null, null, null, 0f, 0f));
        session.SaveChanges();
        var result = session.Query<AutoVecDoc>()
            .Customize(x => x.WaitForNonStaleResults())
            .VectorSearch(x => x.WithText(p => p.Text), v => v.ByText("animal"))
            .Single();
        Assert.NotNull(result.Text);

        result = session.Advanced
            .RawQuery<AutoVecDoc>($"from index 'Auto/AutoVecDocs/ByVector.search(embedding.text(Text))' where 'vector.search(embedding.text(Text))' == null")
            .Single();
        Assert.Null(result.Singles);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void CannotMixVectorAndTextualValues() => CannotMixVectorAndTextualValuesBase<TextualWithVectorMixedField>();
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void CannotMixVectorAndTextualValuesJs() => CannotMixVectorAndTextualValuesBase<TextualWithVectorMixedFieldJs>();
    
    private void CannotMixVectorAndTextualValuesBase<TIndex>()
    where  TIndex : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Store(new AutoVecDoc("test", [.1f, .2f], null, null, 0f, 0f));
        session.SaveChanges();
        new TIndex().Execute(store);
        var errors = Indexes.WaitForIndexingErrors(store);
        Assert.NotNull(errors[0].Errors[0].Error);
        Assert.Contains("tried to index textual value instead", errors[0].Errors[0].Error);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void CannotMixVectorAndNumericalValues() => CannotMixVectorAndNumericalValuesBase<NumericalWithVectorMixedField>();
    public void CannotMixVectorAndNumericalValuesJs() => CannotMixVectorAndNumericalValuesBase<NumericalWithVectorMixedFieldJs>();
    private void CannotMixVectorAndNumericalValuesBase<TIndex>()
    where  TIndex : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Store(new AutoVecDoc(null, [.1f, .2f], [2, -3], null, 0f, 0f));
        session.SaveChanges();
        new TIndex().Execute(store);
        var errors = Indexes.WaitForIndexingErrors(store);
        Assert.NotNull(errors[0].Errors[0].Error);
        Assert.Contains("tried to index numerical value instead", errors[0].Errors[0].Error);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void CannotMixVectorAndSpatialValues() => CannotMixVectorAndSpatialValuesBase<SpatialWithVectorMixedField>();

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void CannotMixVectorAndSpatialValuesJs() => CannotMixVectorAndSpatialValuesBase<SpatialWithVectorMixedFieldJs>();
    
    private void CannotMixVectorAndSpatialValuesBase<TIndex>()
    where TIndex : AbstractIndexCreationTask, new()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenSession();
        session.Store(new AutoVecDoc(null, null, [2, -3], null, 0f, 0f));
        session.SaveChanges();
        new TIndex().Execute(store);
        var errors = Indexes.WaitForIndexingErrors(store);
        Assert.NotNull(errors[0].Errors[0].Error);
        Assert.Contains("tried to index spatial value instead", errors[0].Errors[0].Error);
    }

    private record AutoVecDoc(string Text, float[] Singles, sbyte[] Int8, byte[] Binary, float lat, float lon, string Id = null);

    private class TextualWithVectorMixedField : AbstractIndexCreationTask<AutoVecDoc>
    {
        public TextualWithVectorMixedField()
        {
            Map = docs => from doc in docs
                select new { Vector = doc.Text ?? CreateVector(doc.Singles) };

            Vector("Vector", factory => factory.SourceEmbedding(VectorEmbeddingType.Single).DestinationEmbedding(VectorEmbeddingType.Single));
        }
    }
    
    private class TextualWithVectorMixedFieldJs : AbstractJavaScriptIndexCreationTask
    {
        public TextualWithVectorMixedFieldJs()
        {
            Maps = new HashSet<string>()
            {
                $@"map('AutoVecDocs', function (doc) {{
                return {{
                    Vector: doc.Text == null ? createVector(doc.Singles) : doc.Text
                }};
            }})"
            };

            Fields = new();
            Fields.Add("Vector", new IndexFieldOptions { Vector = new VectorOptions() { SourceEmbeddingType = VectorEmbeddingType.Single, DestinationEmbeddingType = VectorEmbeddingType.Single } });
        }
    }

    private class NumericalWithVectorMixedField : AbstractIndexCreationTask<AutoVecDoc>
    {
        public NumericalWithVectorMixedField()
        {
            Map = docs => from doc in docs
                select new { Vector = doc.Int8 ?? CreateVector(doc.Singles) };

            Vector("Vector", factory => factory.SourceEmbedding(VectorEmbeddingType.Single).DestinationEmbedding(VectorEmbeddingType.Single));
        }
    }
    
    private class NumericalWithVectorMixedFieldJs : AbstractJavaScriptIndexCreationTask
    {
        public NumericalWithVectorMixedFieldJs()
        {
            Maps = new HashSet<string>()
            {
                $@"map('AutoVecDocs', function (dto) {{
                return {{
                    Vector: dto.Int8 ?? createVector(dto.Singles)
                }};
            }})"
            };

            Fields = new();
            Fields.Add("Vector", new IndexFieldOptions { Vector = new VectorOptions() { SourceEmbeddingType = VectorEmbeddingType.Single, DestinationEmbeddingType = VectorEmbeddingType.Single } });
        }
    }

    private class SpatialWithVectorMixedField : AbstractIndexCreationTask<AutoVecDoc>
    {
        public SpatialWithVectorMixedField()
        {
            Map = docs => from doc in docs
                select new { Vector = doc.Singles == null ? CreateSpatialField(doc.lat, doc.lon) : CreateVector(doc.Singles) };

            Vector("Vector", factory => factory.SourceEmbedding(VectorEmbeddingType.Single).DestinationEmbedding(VectorEmbeddingType.Single));
        }
    }
    
    private class SpatialWithVectorMixedFieldJs : AbstractJavaScriptIndexCreationTask
    {
        public SpatialWithVectorMixedFieldJs()
        {
            Maps = new HashSet<string>()
            { $@"map('AutoVecDocs', function (doc) {{
                return {{
                    Vector: doc.Singles == null ? createSpatialField(doc.lat, doc.lon) : createVector(doc.Singles)
                }};
            }})"
            };

            Fields = new();
            Fields.Add("Vector", new IndexFieldOptions { Vector = new VectorOptions() { SourceEmbeddingType = VectorEmbeddingType.Single, DestinationEmbeddingType = VectorEmbeddingType.Single } });
        }
    }
}
