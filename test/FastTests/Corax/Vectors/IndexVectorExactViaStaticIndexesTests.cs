﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Vectors;

public class IndexVectorExactViaStaticIndexesTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Corax)]
    public async Task AssertIndexDefinitionViaStaticIndexes()
    {
        using var store = CreateDocumentStore();
        var localIndexDefinition = new IndexDefinition()
        {
            Name = "Vector",
            Maps = new HashSet<string>()
            {
                @"from doc in docs.Vector
select new 
{
    vec = CreateVector(doc.Vector)
}
"
            },
            Fields = new Dictionary<string, IndexFieldOptions>()
            {
                {
                    "Vec",
                    new IndexFieldOptions()
                    {
                        Vector = new VectorOptions()
                        {
                            DestinationEmbeddingType = VectorEmbeddingType.Binary,
                            SourceEmbeddingType = VectorEmbeddingType.Binary,
                            Dimensions = 1
                        }
                    }
                }
            }
        };

        await store.Maintenance.SendAsync(new PutIndexesOperation(localIndexDefinition));

        var indexDefinitionFromServer = (await store.Maintenance.SendAsync(new GetIndexesOperation(0, 1))).First();


        var cmp = localIndexDefinition.Compare(indexDefinitionFromServer);
        Assert.Equal(IndexDefinitionCompareDifferences.None, cmp);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [InlineData(VectorEmbeddingType.Binary, 0.7f)]
    [InlineData(VectorEmbeddingType.Int8, 0.82f)]
    [InlineData(VectorEmbeddingType.Single, 0.75f)]
    public async Task CanCreateVectorIndexFromCSharp(VectorEmbeddingType vectorEmbeddingType, float similarity)
    => await CanCreateVectorIndexBase<TextVectorIndex>(vectorEmbeddingType, similarity);
    
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [InlineData(VectorEmbeddingType.Binary, 0.7f)]
    [InlineData(VectorEmbeddingType.Int8, 0.82f)]
    [InlineData(VectorEmbeddingType.Single, 0.75f)]
    public async Task CanCreateVectorIndexFromJs(VectorEmbeddingType vectorEmbeddingType, float similarity)
        => await CanCreateVectorIndexBase<TextVectorIndexJs>(vectorEmbeddingType, similarity);

    private async Task CanCreateVectorIndexBase<TIndex>(VectorEmbeddingType vectorEmbeddingType, float similarity)
        where TIndex : AbstractIndexCreationTask, new()
    {
        using var store = CreateDocumentStore();
        
        {
            using var session = store.OpenAsyncSession();
            await session.StoreAsync(new Document() { Text = "Cat has brown eyes." });
            await session.StoreAsync(new Document() { Text = "Apple usually is red." });
            await session.SaveChangesAsync();
        }

        if (typeof(TIndex) == typeof(TextVectorIndexJs))
            await new TextVectorIndexJs(vectorEmbeddingType).ExecuteAsync(store);
        else
            await new TextVectorIndex(vectorEmbeddingType).ExecuteAsync(store);
        await Indexes.WaitForIndexingAsync(store);
        Assert.Null(Indexes.WaitForIndexingErrors(store, errorsShouldExists: false));

        {
            using var session = store.OpenAsyncSession();
            var res = session.Query<Document, TIndex>().VectorSearch(x => x.WithField(f => f.Vector), f => f.ByText("animal"), similarity);
            var results = await res.ToListAsync();

            Assert.Equal(1, results.Count);
            Assert.Contains("Cat", results[0].Text);
        }
    }


    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Vector)]
    public async Task CreateVectorIndexFromFloatEmbeddings() => await CreateVectorIndexFromFloatEmbeddingsBase<NumericalVectorIndex>();
    public async Task CreateVectorIndexFromFloatEmbeddingsJs() => await CreateVectorIndexFromFloatEmbeddingsBase<NumericalVectorIndexJs>();
    
    private async Task CreateVectorIndexFromFloatEmbeddingsBase<TIndex>()
        where TIndex : AbstractIndexCreationTask, new()
    {
        using var store = CreateDocumentStore();
        
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Document() { Embeddings = [0.5f, 0.4f] });
            await session.StoreAsync(new Document() { Embeddings = [0.1f, -0.1f] });
            await session.StoreAsync(new Document() { Embeddings = [-0.1f, -0.1f] });
            await session.SaveChangesAsync();
            await new TIndex().ExecuteAsync(store);
        }

        await Indexes.WaitForIndexingAsync(store);
        Assert.Null(Indexes.WaitForIndexingErrors(store, errorsShouldExists: false));

        using (var session = store.OpenAsyncSession())
        {
            var res = session.Query<Document, TIndex>().VectorSearch(x => x.WithField(f => f.Vector), f => f.ByEmbedding([0.5f, 0.4f]), 0.99f);
            var results = await res.ToListAsync();
            Assert.Equal(1, results.Count);
            Assert.Contains(0.5f, results[0].Embeddings);
        }
    }

    private IDocumentStore CreateDocumentStore() => GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));

    private class TextVectorIndex : AbstractIndexCreationTask<Document>
    {
        public TextVectorIndex()
        {
            //querying
        }

        public TextVectorIndex(VectorEmbeddingType vectorEmbeddingType)
        {
            Map = docs => from doc in docs
                select new { Id = doc.Id, Vector = CreateVector(doc.Text) };


            VectorIndexes.Add(x => x.Vector,
                new VectorOptions()
                {
                    SourceEmbeddingType = VectorEmbeddingType.Text, DestinationEmbeddingType = vectorEmbeddingType
                });
        }
    }
    
    private class TextVectorIndexJs : AbstractJavaScriptIndexCreationTask
    {
        public TextVectorIndexJs()
        {
            //querying
        }

        public TextVectorIndexJs(VectorEmbeddingType vectorEmbeddingType)
        {
            Maps = new HashSet<string>()
            {
                $@"map('Documents', function (dto) {{
                return {{
                    Vector: createVector(dto.Text)
                }};
            }})"
            };
            
            Fields = new();
            Fields.Add("Vector", new IndexFieldOptions()
            {
                Vector = new VectorOptions()
                {
                    SourceEmbeddingType = VectorEmbeddingType.Text, 
                    DestinationEmbeddingType = vectorEmbeddingType
                }
            });
        }
    }

    private class NumericalVectorIndex : AbstractIndexCreationTask<Document>
    {
        public NumericalVectorIndex()
        {
            Map = docs => from doc in docs
                select new { Id = doc.Id, Vector = CreateVector(doc.Embeddings) };


            VectorIndexes.Add(x => x.Vector, new VectorOptions() { SourceEmbeddingType = VectorEmbeddingType.Single });
        }
    }
    
    private class NumericalVectorIndexJs : AbstractJavaScriptIndexCreationTask
    {
        public NumericalVectorIndexJs()
        {
            Maps = new HashSet<string>()
            {
                $@"map('Documents', function (dto) {{
                return {{
                    Vector: createVector(dto.Embeddings)
                }};
            }})"
            };
            
            Fields = new();
            Fields.Add("Vector", new IndexFieldOptions()
            {
                Vector = new VectorOptions()
                {
                    SourceEmbeddingType = VectorEmbeddingType.Single, 
                    DestinationEmbeddingType = VectorEmbeddingType.Single
                }
            });
        }
    }

    private class Document
    {
        public string Id { get; set; }
        public string Text { get; set; }
        public float[] Embeddings { get; set; }
        public object Vector { get; set; }
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void EmbeddingTextSourceTest() => StaticIndexApi<EmbeddingTextSource>();
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void EmbeddingTextSourceTestJs() => StaticIndexApi<EmbeddingTextSourceJs>();

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void MultiEmbeddingTextIndexTest() => StaticIndexApi<MultiEmbeddingTextIndex>();
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void MultiEmbeddingTextIndexTestJs() => StaticIndexApi<MultiEmbeddingTextIndexJs>();

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void EmbeddingSingleIndexTest() => StaticIndexApi<EmbeddingSingleIndex>();
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void EmbeddingSingleIndexTestJs() => StaticIndexApi<EmbeddingSingleIndexJs>();
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void MultiEmbeddingSingleIndexTest() => StaticIndexApi<MultiEmbeddingSingleIndex>();
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void MultiEmbeddingSingleIndexTestJs() => StaticIndexApi<MultiEmbeddingSingleIndexJs>();

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void EmbeddingSingleAsBase64IndexTest() => StaticIndexApi<EmbeddingSingleAsBase64Index>();
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void EmbeddingSingleAsBase64IndexTestJs() => StaticIndexApi<EmbeddingSingleAsBase64IndexJs>();

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void MultiEmbeddingSingleAsBase64IndexTest() => StaticIndexApi<MultiEmbeddingSingleAsBase64Index>();
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    public void MultiEmbeddingSingleAsBase64IndexTestJs() => StaticIndexApi<MultiEmbeddingSingleAsBase64IndexJs>();

    private void StaticIndexApi<TIndex>() where TIndex : AbstractIndexCreationTask, new()
    {
        using var store = CreateDocumentStore();
        using (var session = store.OpenSession())
        {
            float[][] embeddings = [[0.1f, 0.1f], [0.2f, 0.3f]];
            var embAsByte = MemoryMarshal.Cast<float, byte>(embeddings[0]).ToArray();
            var embAsByte2 = MemoryMarshal.Cast<float, byte>(embeddings[1]).ToArray();
            session.Store(new DataSource()
            {
                Text = "cat",
                MultiText = ["cat", "dog"],
                Embeddings = embeddings[0],
                MultipleEmbeddings = embeddings,
                EmbeddingAsBase64 = Convert.ToBase64String(embAsByte),
                EmbeddingsAsBase64 = [Convert.ToBase64String(embAsByte), Convert.ToBase64String(embAsByte2)]
            });
            session.SaveChanges();
        }

        new TIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        Assert.Equal(0, GetErrorCounts());

        int GetErrorCounts()
        {
            var errors = store.Maintenance.Send(new GetIndexErrorsOperation());
            return errors.First(x => x.Name == new TIndex().IndexName).Errors.Length;
        }
    }

    private class DataSource
    {
        public string Id { get; set; }
        public string Text { get; set; }
        public string[] MultiText { get; set; }
        public float[] Embeddings { get; set; }
        public float[][] MultipleEmbeddings { get; set; }
        public string EmbeddingAsBase64 { get; set; }
        public string[] EmbeddingsAsBase64 { get; set; }
        public object Vector { get; set; }
    }


    private class EmbeddingTextSource : AbstractIndexCreationTask<DataSource>
    {
        public EmbeddingTextSource()
        {
            Map = sources => sources.Select(x => new { Vector = CreateVector(x.Text) });

            VectorIndexes.Add(source => source.Vector,
                new VectorOptions()
                {
                    SourceEmbeddingType = VectorEmbeddingType.Text, DestinationEmbeddingType = VectorEmbeddingType.Single
                });
        }
    }
    
    private class EmbeddingTextSourceJs : AbstractJavaScriptIndexCreationTask
    {
        public EmbeddingTextSourceJs()
        {
            Maps = new HashSet<string>()
            {
                $@"map('DataSources', function (dto) {{
                return {{
                    Vector: createVector(dto.Text)
                }};
            }})"
            };

            Fields = new();
            Fields.Add("Vector", new IndexFieldOptions()
            {
                Vector = new VectorOptions()
                {
                    SourceEmbeddingType = VectorEmbeddingType.Text, 
                    DestinationEmbeddingType = VectorEmbeddingType.Single
                }
            });
        }
    }

    private class MultiEmbeddingTextIndex : AbstractIndexCreationTask<DataSource>
    {
        public MultiEmbeddingTextIndex()
        {
            Map = sources => sources.Select(x => new { Vector = CreateVector(x.MultiText) });

            VectorIndexes.Add(source => source.Vector,
                new VectorOptions()
                {
                    SourceEmbeddingType = VectorEmbeddingType.Text, DestinationEmbeddingType = VectorEmbeddingType.Single
                });
        }
    }
    
    private class MultiEmbeddingTextIndexJs : AbstractJavaScriptIndexCreationTask
    {
        public MultiEmbeddingTextIndexJs()
        {
            Maps = new HashSet<string>()
            {
                $@"map('DataSources', function (dto) {{
                return {{
                    Vector: createVector(dto.MultiText)
                }};
            }})"
            };

            Fields = new();
            Fields.Add("Vector", new IndexFieldOptions()
            {
                Vector = new VectorOptions()
                {
                    SourceEmbeddingType = VectorEmbeddingType.Text, 
                    DestinationEmbeddingType = VectorEmbeddingType.Single
                }
            });
        }
    }

    private class EmbeddingSingleIndex : AbstractIndexCreationTask<DataSource>
    {
        public EmbeddingSingleIndex()
        {
            Map = sources => sources.Select(x => new { Vector = CreateVector(x.Embeddings) });

            VectorIndexes.Add(source => source.Vector,
                new VectorOptions()
                {
                    SourceEmbeddingType = VectorEmbeddingType.Single, DestinationEmbeddingType = VectorEmbeddingType.Single
                });
        }
    }
    
    private class EmbeddingSingleIndexJs : AbstractJavaScriptIndexCreationTask
    {
        public EmbeddingSingleIndexJs()
        {
            Maps = new HashSet<string>()
            {
                $@"map('DataSources', function (dto) {{
                return {{
                    Vector: createVector(dto.Embeddings)
                }};
            }})"
            };

            Fields = new();
            Fields.Add("Vector", new IndexFieldOptions()
            {
                Vector = new VectorOptions()
                {
                    SourceEmbeddingType = VectorEmbeddingType.Single, 
                    DestinationEmbeddingType = VectorEmbeddingType.Single
                }
            });
        }
    }

    private class MultiEmbeddingSingleIndex : AbstractIndexCreationTask<DataSource>
    {
        public MultiEmbeddingSingleIndex()
        {
            Map = sources => sources.Select(x => new { Vector = CreateVector(x.MultipleEmbeddings) });

            VectorIndexes.Add(source => source.Vector,
                new VectorOptions()
                {
                    SourceEmbeddingType = VectorEmbeddingType.Single, DestinationEmbeddingType = VectorEmbeddingType.Single
                });
        }
    }
    
    private class MultiEmbeddingSingleIndexJs : AbstractJavaScriptIndexCreationTask
    {
        public MultiEmbeddingSingleIndexJs()
        {
            Maps = new HashSet<string>()
            {
                $@"map('DataSources', function (dto) {{
                return {{
                    Vector: createVector(dto.MultipleEmbeddings)
                }};
            }})"
            };

            Fields = new();
            Fields.Add("Vector", new IndexFieldOptions()
            {
                Vector = new VectorOptions()
                {
                    SourceEmbeddingType = VectorEmbeddingType.Single, 
                    DestinationEmbeddingType = VectorEmbeddingType.Single
                }
            });
        }
    }

    private class EmbeddingSingleAsBase64Index : AbstractIndexCreationTask<DataSource>
    {
        public EmbeddingSingleAsBase64Index()
        {
            Map = sources => sources.Select(x => new { Vector = CreateVector(x.EmbeddingAsBase64) });

            VectorIndexes.Add(source => source.Vector,
                new VectorOptions()
                {
                    SourceEmbeddingType = VectorEmbeddingType.Single, DestinationEmbeddingType = VectorEmbeddingType.Single
                });
        }
    }
    
    private class EmbeddingSingleAsBase64IndexJs : AbstractJavaScriptIndexCreationTask
    {
        public EmbeddingSingleAsBase64IndexJs()
        {
            Maps = new HashSet<string>()
            {
                $@"map('DataSources', function (dto) {{
                return {{
                    Vector: createVector(dto.EmbeddingAsBase64)
                }};
            }})"
            };

            Fields = new();
            Fields.Add("Vector", new IndexFieldOptions()
            {
                Vector = new VectorOptions()
                {
                    SourceEmbeddingType = VectorEmbeddingType.Single, 
                    DestinationEmbeddingType = VectorEmbeddingType.Single
                }
            });
        }
    }

    private class MultiEmbeddingSingleAsBase64Index : AbstractIndexCreationTask<DataSource>
    {
        public MultiEmbeddingSingleAsBase64Index()
        {
            Map = sources => sources.Select(x => new { Vector = CreateVector(x.EmbeddingsAsBase64) });

            VectorIndexes.Add(source => source.Vector,
                new VectorOptions()
                {
                    SourceEmbeddingType = VectorEmbeddingType.Single, DestinationEmbeddingType = VectorEmbeddingType.Single
                });
        }
    }
    
    private class MultiEmbeddingSingleAsBase64IndexJs : AbstractJavaScriptIndexCreationTask
    {
        public MultiEmbeddingSingleAsBase64IndexJs()
        {
            Maps = new HashSet<string>()
            {
                $@"map('DataSources', function (dto) {{
                return {{
                    Vector: createVector(dto.EmbeddingsAsBase64)
                }};
            }})"
            };

            Fields = new();
            Fields.Add("Vector", new IndexFieldOptions()
            {
                Vector = new VectorOptions()
                {
                    SourceEmbeddingType = VectorEmbeddingType.Single, 
                    DestinationEmbeddingType = VectorEmbeddingType.Single
                }
            });
        }
    }
}
