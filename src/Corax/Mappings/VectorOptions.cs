﻿using Voron.Data.Graphs;

namespace Corax.Mappings;

public class VectorOptions
{
    public int NumberOfCandidates { get; init; }
    public int NumberOfEdges { get; init; }
    public VectorEmbeddingType VectorEmbeddingType { get; init; }
}
