﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron.Data.BTrees;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Global;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;
using Container = Voron.Data.Containers.Container;

namespace Voron.Data.Graphs;

public unsafe partial class Hnsw
{
    public ref struct NodeReader(ByteStringContext allocator, Span<byte> buffer)
    {
        public long PostingListId;
        public long VectorId;
        public int CountOfLevels;

        private int _offset;
        private readonly Span<byte> _buffer = buffer;

        public void LoadInto(ref Node node)
        {
            node.VectorId = VectorId;
            node.PostingListId = PostingListId;
            node.EdgesPerLevel.EnsureCapacityFor(allocator, CountOfLevels);
            while (NextReadEdges(out var list))
            {
                node.EdgesPerLevel.AddUnsafe(list);
            }
        }

        private bool NextReadEdges(out NativeList<long> list)
        {
            if (_offset >= _buffer.Length)
            {
                list = default;
                return false;
            }

            var count = VariableSizeEncoding.Read<int>(_buffer, out int offset, _offset);
            _offset += offset;
            list = new NativeList<long>();
            list.EnsureCapacityFor(allocator, count);
            long prev = 0;
            for (int i = 0; i < count; i++)
            {
                var item = VariableSizeEncoding.Read<long>(_buffer, out offset, _offset);
                _offset += offset;
                prev += item;
                Debug.Assert(prev >= 0, "prev >= 0");
                list.AddUnsafe(prev);
            }
            return true;
        }

        public UnmanagedSpan ReadVector(in SearchState state) => ReadVector(VectorId, in state);

        public static UnmanagedSpan ReadVector(long vectorId, in SearchState state)
        {
            if ((vectorId & 1) == 0)
            {
                var item = Container.Get(state.Llt, vectorId);
                var vectorSpan = new UnmanagedSpan(item.Address, item.Length);
                Debug.Assert(state.Options.VectorSizeBytes == vectorSpan.Length, "state.Options.VectorSizeBytes == vectorSpan.Length");
                return vectorSpan;
            }
            
            var count = (byte)(vectorId >> 1);
            var containerId = vectorId & ~0xFFF;
            var container = Container.Get(state.Llt, containerId);
            var offset = count * state.Options.VectorSizeBytes;
            Debug.Assert(offset >= 0 && offset + state.Options.VectorSizeBytes <= container.Length, "offset >= 0 && offset + state.Options.VectorSizeBytes <= container.Length");
            return new UnmanagedSpan(container.Address + offset, state.Options.VectorSizeBytes);
        }
    }

    public struct Node
    {
        public long PostingListId;
        public long VectorId;
        public long NodeId;
        public NativeList<NativeList<long>> EdgesPerLevel;
        private UnmanagedSpan _vectorSpan;
        public int Visited;

        public static NodeReader Decode(LowLevelTransaction llt, long id)
        {
            var span = Container.Get(llt, id).ToSpan();
            return Decode(llt, span);
        }

        public static NodeReader Decode(LowLevelTransaction llt, Span<byte> span)
        {
            var postingListId = VariableSizeEncoding.Read<long>(span, out var pos);
            var offset = pos;
            var vectorId = VariableSizeEncoding.Read<long>(span, out pos, offset);
            offset += pos;
            var countOfLevels = VariableSizeEncoding.Read<int>(span, out pos, offset);
            offset += pos;

            return new NodeReader(llt.Allocator, span[offset..])
            {
                PostingListId = postingListId,
                VectorId = vectorId,
                CountOfLevels = countOfLevels
            };
        }

        public Span<byte> Encode(ref ContextBoundNativeList<byte> buffer)
        {
            int countOfLevels = EdgesPerLevel.Count;

            // posting list id, vector id, count of levels
            var maxSize = 3 * VariableSizeEncoding.MaximumSizeOf<long>();
            for (int i = 0; i < countOfLevels; i++)
            {
                maxSize += EdgesPerLevel[i].Count * VariableSizeEncoding.MaximumSizeOf<long>();
            }
            buffer.EnsureCapacityFor(maxSize);

            var bufferSpan = buffer.ToFullCapacitySpan();
            
            var pos = VariableSizeEncoding.Write(bufferSpan, PostingListId);
            pos += VariableSizeEncoding.Write(bufferSpan, VectorId, pos);
            pos += VariableSizeEncoding.Write(bufferSpan, countOfLevels, pos);
            
            for (int i = 0; i < countOfLevels; i++)
            {
                Span<long> span = EdgesPerLevel[i].ToSpan();
                int len = Sorting.SortAndRemoveDuplicates(span);
                span = span[..len];
                long prev = 0;
                pos += VariableSizeEncoding.Write(bufferSpan, span.Length, pos);
                for (int j = 0; j < span.Length; j++)
                {
                    var delta = span[j] - prev;
                    prev = span[j];
                    pos += VariableSizeEncoding.Write(bufferSpan, delta, pos);
                }
            }

            return bufferSpan[..pos];
        }

        public Span<byte> GetVector(SearchState state)
        {
            if (_vectorSpan.Length > 0) 
                return _vectorSpan.ToSpan();

            _vectorSpan = NodeReader.ReadVector(VectorId, in state);
            return _vectorSpan;
        }
    }

    public static void Create(LowLevelTransaction llt, string name, int vectorSizeBytes, int numberOfEdges, int numberOfCandidates, VectorEmbeddingType embeddingType)
    {
        using var _ = Slice.From(llt.Allocator, name, out var slice);
        Create(llt, slice, vectorSizeBytes, numberOfEdges, numberOfCandidates, embeddingType);
    }
    
    public static void Create(LowLevelTransaction llt, Slice name, int vectorSizeBytes, int numberOfEdges, int numberOfCandidates, VectorEmbeddingType embeddingType)
    {
        var tree = llt.Transaction.CreateTree(name);
        if (tree.State.Header.NumberOfEntries is not 0)
            return; // already created

        // global creation for all HNSWs in the database
        var vectorsContainerId = CreateHnswGlobalState(llt);
        long storage = Container.Create(llt);
        tree.LookupFor<Int64LookupKey>(NodeIdToLocationSlice);
        tree.LookupFor<Int64LookupKey>(NodesByVectorIdSlice);

        var similarityMethod = embeddingType switch
        {
            VectorEmbeddingType.Single => SimilarityMethod.CosineSimilaritySingles,
            VectorEmbeddingType.Int8 => SimilarityMethod.CosineSimilarityI8,
            VectorEmbeddingType.Binary => SimilarityMethod.HammingDistance,
            _ => throw new InvalidOperationException($"Unexpected value of {nameof(VectorEmbeddingType)}: {embeddingType}")
        };
        
        var options = new Options
        {
            Version = 1,
            VectorSizeBytes = vectorSizeBytes,
            CountOfVectors = 0,
            Container = storage,
            NumberOfEdges = numberOfEdges,
            NumberOfCandidates = numberOfCandidates,
            SimilarityMethod = similarityMethod
        };
        using (tree.DirectAdd(OptionsSlice, sizeof(Options), out var output))
        {
            Unsafe.Write(output, options);
        }
    }

    private static long ReadGlobalVectorsContainerId(LowLevelTransaction llt)
    {
        var config = llt.Transaction.ReadTree(HnswGlobalConfigSlice);
        var read = config.DirectRead(VectorsContainerIdSlice);
        return Unsafe.Read<long>(read);
    }

    private static long CreateHnswGlobalState(LowLevelTransaction llt)
    {
        llt.Transaction.CompactTreeFor(VectorsIdByHashSlice);
        var config = llt.Transaction.CreateTree(HnswGlobalConfigSlice);
        var read = config.DirectRead(VectorsContainerIdSlice);
        if (read is not null)
            return Unsafe.Read<long>(read);

        long vectorsContainerId = Container.Create(llt);
        config.Add(VectorsContainerIdSlice, vectorsContainerId);
        return vectorsContainerId;
    }

    public class SearchState
    {
        private readonly PriorityQueue<int, float> _candidatesQ = new();
        private readonly PriorityQueue<int, float> _nearestEdgesQ = new();
        private readonly Dictionary<long, int> _nodeIdToIdx = new();
        private NativeList<Node> _nodes = default;
        private readonly Tree _tree;
        private readonly Lookup<Int64LookupKey> _nodeIdToLocations;
        public readonly LowLevelTransaction Llt;
        private int _visitsCounter;
        public readonly delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, float> SimilarityCalc;
        public readonly bool IsEmpty;
        
        
        public Span<Node> Nodes => _nodes.ToSpan();
        public Tree Tree => _tree;

        public int CreatedNodesCount;

        public Options Options;

        public SearchState(LowLevelTransaction llt, string name): this(llt, SliceFromString(llt, name))
        {
        }

        private static Slice SliceFromString(LowLevelTransaction llt, string name)
        {
            Slice.From(llt.Allocator, name, out var slice);
            return slice;
        }

        public SearchState(LowLevelTransaction llt, Slice name)
        {
            Llt = llt;
            _tree = llt.Transaction.ReadTree(name);

            if (_tree is null || _tree.TryGetLookupFor(NodeIdToLocationSlice, out _nodeIdToLocations) == false)
            {
                IsEmpty = true;
                return;
            }
            
            var options = _tree.DirectRead(OptionsSlice);
            Options = Unsafe.Read<Options>(options);
            SimilarityCalc = Options.SimilarityMethod switch
            {
                SimilarityMethod.CosineSimilaritySingles => &CosineSimilaritySingles,
                SimilarityMethod.CosineSimilarityI8 => &CosineSimilarityI8,
                SimilarityMethod.HammingDistance => &HammingDistance,
                _ => throw new ArgumentOutOfRangeException(nameof(Options.SimilarityMethod), Options.SimilarityMethod, null)
            };
        }

        public float MinimumSimilarityToDistance(float minimumSimilarity)
        {
            switch (Options.SimilarityMethod)
            {
                case SimilarityMethod.CosineSimilaritySingles:
                case SimilarityMethod.CosineSimilarityI8:
                    return 2f * (1.0f - minimumSimilarity);
                case SimilarityMethod.HammingDistance:
                    return Options.VectorSizeBytes * 8 * (1f - minimumSimilarity);  // number_of_bits * minimum_similarity
                default:
                    throw new InvalidDataException($"Unknown similarity method {Options.SimilarityMethod}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float DistanceToScore(float score)
        {
            switch (Options.SimilarityMethod)
            {
                case SimilarityMethod.CosineSimilaritySingles:
                case SimilarityMethod.CosineSimilarityI8:
                    return 1 - score;
                case SimilarityMethod.HammingDistance:
                    return ((Options.VectorSizeBytes * 8) - score) / (8f * Options.VectorSizeBytes);  // number_of_bits * minimum_similarity
                default:
                    throw new InvalidDataException($"Unknown similarity method {Options.SimilarityMethod}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DistancesToScores(Span<float> distances)
        {
            switch (Options.SimilarityMethod)
            {
                case SimilarityMethod.CosineSimilaritySingles:
                case SimilarityMethod.CosineSimilarityI8:
                    DistanceToScoreCosineSimilarity(distances);
                    break;
                case SimilarityMethod.HammingDistance:
                    DistanceToScoreHammingSimilarity(distances, Options.VectorSizeBytes);
                    break;
                default:
                    throw new InvalidDataException($"Unknown similarity method {Options.SimilarityMethod}");
            }
        }
        
        public void FlushOptions()
        {
            using (_tree.DirectAdd(OptionsSlice, sizeof(Options), out var dst))
            {
                Unsafe.Write(dst, Options);
            }
        }
        
        public int RegisterVectorNode(long newNodeId, long vectorId)
        {
            CreatedNodesCount++;
            int nodeIndex = AllocateNodeIndex(newNodeId);
            _nodes[nodeIndex].VectorId = vectorId;

            _nodeIdToIdx[newNodeId] = nodeIndex;
            return nodeIndex;
        }

        private int AllocateNodeIndex(long nodeId)
        {
            int nodeIndex = _nodes.Count;
            _nodes.Add(Llt.Allocator, new Node { NodeId = nodeId });
            return nodeIndex;
        }

        public bool TryGetLocationForNode(long nodeId, out long locationId) =>
            _nodeIdToLocations.TryGetValue(nodeId, out locationId);

        public void RegisterNodeLocation(long nodeId, long locationId) =>
            _nodeIdToLocations.Add(nodeId, locationId);

        public ref Node GetNodeByIndex(int index)
        {
            ref var n = ref _nodes[index];
            Debug.Assert(n.NodeId is not 0, "n.NodeId is not 0");
            return ref n;
        }

        public void ReadNode(long nodeId, out NodeReader n)
        {
            if (TryGetLocationForNode(nodeId, out var nodeLocation) is false)
                throw new InvalidOperationException($"Unable to find node id {nodeId}");
            n = Node.Decode(Llt, nodeLocation);
        }

        /// <summary>
        /// This accepts a list of node ids (mutable, we do destructive to it) and translate
        /// that to a list of the indexes in the nodes array. If needed, it will load the nodes
        /// from the disk in a batch oriented manner. 
        /// </summary>
        private void LoadNodeIndexes(ref NativeList<long> nodeIds, ref NativeList<int> indexes)
        {
            indexes.ResetAndEnsureCapacity(Llt.Allocator, nodeIds.Count);
            for (int i = 0; i < nodeIds.Count; i++)
            {
                if (_nodeIdToIdx.TryGetValue(nodeIds[i], out var index))
                {
                    indexes.AddUnsafe(index);
                    nodeIds[i] = -1;
                }
            }
            
            if (indexes.Count == nodeIds.Count)
                return;

            var matches = indexes.Count;
            var keys = nodeIds.ToSpan();
            keys.Sort();
            keys = keys[matches..]; // discard all those we already found
            for (int i = 0; i < keys.Length; i++)
            {
                var nodeIdx = AllocateNodeIndex(keys[i]);
                _nodes[nodeIdx].NodeId = keys[i];
                _nodeIdToIdx[keys[i]] = nodeIdx;
                indexes.AddUnsafe(nodeIdx);
            }
            _nodeIdToLocations.GetFor(keys, keys, -1);
            
            using var _ = Llt.Allocator.AllocateDirect(sizeof(UnmanagedSpan) * keys.Length, out var buffer);
            var spans = (UnmanagedSpan*)buffer.Ptr;
            Container.GetAll(Llt, keys, spans, -1, Llt.PageLocator);
            for (int i = 0; i < keys.Length; i++)
            {
                var buf = spans[i].ToSpan();
                var reader = Node.Decode(Llt, buf);
                reader.LoadInto(ref _nodes[indexes[matches + i]]);
            }
        }

        public int GetNodeIndexById(long nodeId)
        {
            ref var nodeIdx = ref CollectionsMarshal.GetValueRefOrAddDefault(_nodeIdToIdx, nodeId, out var exists);
            if (exists)
                return nodeIdx;
            
            if (TryGetLocationForNode(nodeId, out var nodeLocation) is false)
                throw new InvalidOperationException($"Unable to find node id {nodeId}");

            nodeIdx =  AllocateNodeIndex(nodeId);
            var reader = Node.Decode(Llt, nodeLocation);
            ref var n = ref GetNodeByIndex(nodeIdx);
            reader.LoadInto(ref n);
            return nodeIdx;
        }
        
        public ref Node GetNodeById(long nodeId)
        {
            int idx = GetNodeIndexById(nodeId);
            return ref GetNodeByIndex(idx);
        }

        public float Distance(ReadOnlySpan<byte> vector, int fromIdx, int toIdx)
        {
            if (vector.IsEmpty)
            {
                ref var from = ref GetNodeByIndex(fromIdx);
                vector = from.GetVector(this); // note we've to make a copy here since we cannot pass this as ref into ref value
            }

            ref var to = ref GetNodeByIndex(toIdx);
            Span<byte> v2 = to.GetVector(this);
            var distance = SimilarityCalc(vector, v2);
            return distance;
        }
        
        public void ReadPostingList(long rawPostingListId, ref ContextBoundNativeList<long> listBuffer, ref FastPForDecoder pforDecoder, out int postingListSize)
        {
            var smallPostingList = Container.Get(Llt, rawPostingListId);
            var count = VariableSizeEncoding.Read<int>(smallPostingList.Address, out var offset);
            
            var requiredSize = Math.Max(256, 256 * (int)Math.Ceiling((count + listBuffer.Count) / 256f));
            listBuffer.EnsureCapacityFor(requiredSize);
            Debug.Assert(listBuffer.Capacity > 0 && listBuffer.Capacity % 256 ==0, "The buffer must be multiple of 256 for PForDecoder.Read");
            
            pforDecoder.Init(smallPostingList.Address + offset, smallPostingList.Length - offset);
            listBuffer.Count += pforDecoder.Read(listBuffer.RawItems + listBuffer.Count, listBuffer.Capacity - listBuffer.Count);
            postingListSize = smallPostingList.Length;
        }

        public void FilterEdgesHeuristic(int srcIdx, ref NativeList<int> candidates)
        {
            // See: https://icode.best/i/45208840268843 - Chinese, but auto-translate works, and a good explanation with 
            // conjunction of: https://img-bc.icode.best/20210425010212938.png
            // See also the paper here: https://arxiv.org/pdf/1603.09320
            // This implements the Fig. 2 / Algorithm 4
            
            Debug.Assert(_candidatesQ.Count is 0);
            for (int i = 0; i < candidates.Count; i++)
            {
                var dstIndex = candidates[i];
                var distance = Distance(Span<byte>.Empty, srcIdx, dstIndex);
                _candidatesQ.Enqueue(dstIndex, distance);
            }

            candidates.Clear();
            
            while (candidates.Count <= Options.NumberOfEdges &&
                   _candidatesQ.TryDequeue(out var cur, out var distance))
            {
                bool match = true;
                for (int i = 0; i < candidates.Count; i++)
                {
                    int alternativeIndex = candidates[i];
                    var curDist = Distance(Span<byte>.Empty, cur, alternativeIndex);
                    // there is already an item in the result that is *closer* to the current
                    // node than the target node, so no need to add it
                    if (curDist < distance)
                    {
                        match = false;
                        break;
                    }
                }

                if (match)
                {
                    Debug.Assert(candidates.HasCapacityFor(1), "candidates.HasCapacityFor(1)");
                    candidates.AddUnsafe(cur);
                }
            }

            _candidatesQ.Clear();
        }

        [Flags]
        public enum NearestEdgesFlags
        {
            None = 0,
            StartingPointAsEdge = 1 << 1,
            FilterNodesWithEmptyPostingLists = 1 << 2
        }

        public void NearestEdges(int startingPointIndex, 
            int dstIdx, ReadOnlySpan<byte> vector, 
            int level, int numberOfCandidates, 
            ref NativeList<int> candidates,
            NearestEdgesFlags flags)
        {
            Debug.Assert(_candidatesQ.Count == 0, "_candidatesQ.Count == 0");
            Debug.Assert(_nearestEdgesQ.Count == 0, "_nearestEdgesQ.Count == 0");
            
            float lowerBound = -Distance(vector, dstIdx, startingPointIndex);
            var visitedCounter = ++_visitsCounter; 
            ref var startingPoint = ref GetNodeByIndex(startingPointIndex);
            startingPoint.Visited = visitedCounter;
            // candidates queue is sorted using the distance, so the lowest distance
            // will always pop first.
            // nearest edges is sorted using _reversed_ distance, so when we add a 
            // new item to the queue, we'll pop the one with the largest distance
            
            _candidatesQ.Enqueue(startingPointIndex, -lowerBound);
            if (flags.HasFlag(NearestEdgesFlags.StartingPointAsEdge) && 
                    ((startingPoint.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask) != Constants.Graphs.VectorId.Tombstone 
                     || flags.HasFlag(NearestEdgesFlags.FilterNodesWithEmptyPostingLists) is false))
            {
                _nearestEdgesQ.Enqueue(startingPointIndex, lowerBound);
            }
            
            var indexes = new NativeList<int>();
            var nodeIds = new NativeList<long>();
            while (_candidatesQ.TryDequeue(out var cur, out var curDistance))
            {
                if (-curDistance < lowerBound && 
                    _nearestEdgesQ.Count == numberOfCandidates)
                    break;

                ref var candidate = ref GetNodeByIndex(cur);
                candidate.Visited = visitedCounter;
             
                ref var edges = ref candidate.EdgesPerLevel[level];

                nodeIds.ResetAndCopyFrom(Llt.Allocator, edges.ToSpan());
                LoadNodeIndexes(ref nodeIds, ref indexes);

                for (int i = 0; i < indexes.Count; i++)
                {
                    var nextIndex = indexes[i];
                    ref var next = ref GetNodeByIndex(nextIndex);
                    if(next.Visited == visitedCounter)
                        continue; // already checked it
                    next.Visited = visitedCounter;
                    var isDeleted = (next.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask) == Constants.Graphs.VectorId.Tombstone;

                    float nextDist = -Distance(vector, dstIdx, nextIndex);
                    if (_nearestEdgesQ.Count < numberOfCandidates)
                    {
                        _candidatesQ.Enqueue(nextIndex, -nextDist);
                        
                        if (nextIndex != dstIdx && 
                            (isDeleted == false || flags.HasFlag(NearestEdgesFlags.FilterNodesWithEmptyPostingLists) is false))
                        {
                            _nearestEdgesQ.Enqueue(nextIndex, nextDist);
                        }
                    }
                    else if (lowerBound < nextDist)
                    {
                        _candidatesQ.Enqueue(nextIndex, -nextDist);
                        
                        if (nextIndex != dstIdx && 
                            (isDeleted == false || flags.HasFlag(NearestEdgesFlags.FilterNodesWithEmptyPostingLists) is false))
                        {
                            _nearestEdgesQ.EnqueueDequeue(nextIndex, nextDist);
                        }
                    }
                    else
                    {
                        continue;
                    }
                    
                    Debug.Assert(_candidatesQ.Count > 0);
                    _nearestEdgesQ.TryPeek(out _, out lowerBound);
                }
            }

            _candidatesQ.Clear();
            candidates.EnsureCapacityFor(Llt.Allocator, _nearestEdgesQ.Count);
            
            while (_nearestEdgesQ.TryDequeue(out var edgeId, out var d))
            {
                candidates.AddUnsafe(edgeId);
            }

            candidates.Reverse();
            Debug.Assert(candidates.ToSpan().Contains(dstIdx) == false, "candidates.ToSpan().Contains(dstIdx) == false");
            
            nodeIds.Dispose(Llt.Allocator);
            indexes.Dispose(Llt.Allocator);
        }

        public void SearchNearestAcrossLevels(ReadOnlySpan<byte> vector, int dstIdx, int maxLevel, ref NativeList<int> nearestIndexes)
        {
            var visitCounter = ++_visitsCounter;
            var currentNodeIndex = GetNodeIndexById(EntryPointId);
            var level = maxLevel;
            ref var entry = ref GetNodeByIndex(currentNodeIndex);
            entry.EdgesPerLevel.SetCapacity(Llt.Allocator, maxLevel + 1);
            var distance = Distance(vector, dstIdx, currentNodeIndex);
            var indexes = new NativeList<int>();
            var nodeIds = new NativeList<long>();

            while (level >= 0)
            {
                bool moved;
                do
                {
                    moved = false;
                    ref var n = ref GetNodeByIndex(currentNodeIndex);
                    Debug.Assert(n.EdgesPerLevel.Count > level, "n.EdgesPerLevel.Count > level");
                    ref var edges = ref n.EdgesPerLevel[level];
                    nodeIds.ResetAndCopyFrom(Llt.Allocator, edges.ToSpan());
                    LoadNodeIndexes(ref nodeIds, ref indexes);
                    for (var i = 0; i < indexes.Count; i++)
                    {
                        var edgeIdx = indexes[i];
                        ref var edge = ref GetNodeByIndex(edgeIdx);
                        if (edge.Visited == visitCounter)
                            continue; // already checked it
                        edge.Visited = visitCounter;
                        var curDist = Distance(vector, dstIdx, edgeIdx);
                        if (curDist >= distance || double.IsNaN(curDist))
                            continue;

                        moved = true;
                        distance = curDist;
                        currentNodeIndex = edgeIdx;
                    }
                } while (moved);

                nearestIndexes.AddUnsafe(currentNodeIndex);
                level--;
            }
            indexes.Dispose(Llt.Allocator);
            nodeIds.Dispose(Llt.Allocator);
            nearestIndexes.Reverse();
        }
    }
    
    public class Registration : IDisposable
    {
        public bool IsCommited { get; private set; }
        private readonly Dictionary<ByteString, (ByteString Key, int NodeIndex, NativeList<long> PostingList)> _vectorHashCache = new(ByteStringContentComparer.Instance);
        private readonly Lookup<Int64LookupKey> _nodesByVectorId;
        private SearchState _searchState;
        public Random Random;
        private readonly CompactTree _vectorsByHash;
        private readonly int _vectorBatchSizeInPages;
        private readonly long _globalVectorsContainerId;
        private PostingList _largePostingListSet;

        public int AmountOfModifiedVectorsInTransaction => _vectorHashCache.Count;
        
        public Registration(LowLevelTransaction llt, Slice name, Random random = null)
        {
            Random = random ?? Random.Shared;
            _searchState = new SearchState(llt, name);
            _vectorBatchSizeInPages = _searchState.Options.VectorBatchInPages;
            _globalVectorsContainerId = ReadGlobalVectorsContainerId(llt);
            _nodesByVectorId = _searchState.Tree.LookupFor<Int64LookupKey>(NodesByVectorIdSlice);
            _vectorsByHash = llt.Transaction.CompactTreeFor(VectorsIdByHashSlice);
        }

        /// <summary>
        /// Removes a vector from the graph.
        /// </summary>
        /// <param name="entryId">The ID of the document.</param>
        /// <param name="vectorHash">The hash of the vector to remove.</param>
        public void Remove(long entryId, ReadOnlySpan<byte> vectorHash)
        {
            entryId = EntryIdToInternalEntryId(entryId);
            const long RemovalMask = 1;

            PortableExceptions.ThrowIfOnDebug<ArgumentOutOfRangeException>((entryId & Constants.Graphs.VectorId.EnsureIsSingleMask) != 0, "Entry ids must have the first two bits cleared, we are using those");

            _searchState.Llt.Allocator.AllocateDirect(Sodium.GenericHashSize, out var hashBuffer);
            vectorHash.CopyTo(hashBuffer.ToSpan());
            
            ref var postingList = ref CollectionsMarshal.GetValueRefOrAddDefault(_vectorHashCache, hashBuffer, out var exists);
            if (exists)
            {
                ref var l = ref postingList.PostingList;
                l.Add(_searchState.Llt.Allocator, entryId | RemovalMask);
                _searchState.Llt.Allocator.Release(ref hashBuffer);
                return;
            }

            if (_vectorsByHash.TryGetValue(vectorHash, out var vectorId) is false)
                PortableExceptions.Throw<InvalidOperationException>($"Unable to find the vector corresponding to the provided vector hash: base64({Convert.ToBase64String(vectorHash)}).");
            
            if (_nodesByVectorId.TryGetValue(vectorId, out var nodeId) is false)
                PortableExceptions.Throw<InvalidOperationException>($"Unable to find the node corresponding to the provided vector hash: base64({Convert.ToBase64String(vectorHash)}) and VectorId({vectorId}).");

            int nodeIndex = _searchState.GetNodeIndexById(nodeId);
            postingList = (hashBuffer, nodeIndex, NativeList<long>.Create(_searchState.Llt.Allocator, entryId  | RemovalMask));
        }

        /// <summary>
        /// The two lowest bits must be cleared for mask purposes.
        /// </summary>
        /// <param name="entryId">Original entryId.</param>
        /// <returns>Internal Hnsw entryId</returns>
        internal static long EntryIdToInternalEntryId(long entryId)
        {
            Debug.Assert(entryId > 0 && (~(long.MaxValue >> 2) & entryId) == 0, "entryId > 0 && (~(long.MaxValue >> 2) & entryId) == 0");
            return entryId << 2;
        }

        /// <summary>
        /// During indexing, we're shifting each ID 2 bits to the left to use the two lowest bits as a mask placeholder. This is for querying decoding.
        /// </summary>
        /// <param name="entries">Array of internal ids.</param>
        internal static void InternalEntryIdToEntryId(Span<long> entries)
        {
            var entriesPos = 0;
            ref var entriesRef = ref MemoryMarshal.GetReference(entries);
            
            if (AdvInstructionSet.IsAcceleratedVector512)
            {
                var N = Vector512<long>.Count;
                
                for (; entriesPos + N < entries.Length; entriesPos += N)
                {
                    ref var currentMemory = ref Unsafe.Add(ref entriesRef, entriesPos);
                    var current = Vector512.LoadUnsafe(ref currentMemory);
                    Vector512.ShiftRightLogical(current, 2).StoreUnsafe(ref currentMemory);
                }
            }
            
            if (AdvInstructionSet.IsAcceleratedVector256)
            {
                var N = Vector256<long>.Count;
                
                for (; entriesPos + N < entries.Length; entriesPos += N)
                {
                    ref var currentMemory = ref Unsafe.Add(ref entriesRef, entriesPos);
                    var current = Vector256.LoadUnsafe(ref currentMemory);
                    Vector256.ShiftRightLogical(current, 2).StoreUnsafe(ref currentMemory);
                }
            }

            for (; entriesPos < entries.Length; entriesPos++)
                Unsafe.Add(ref entriesRef, entriesPos) >>= 2;
        }
        
        /// <summary>
        /// Adds a vector to the graph.
        /// </summary>
        /// <param name="entryId">The ID of the document (source).</param>
        /// <param name="vector">The vector's data.</param>
        /// <returns>The CompactKey address of the hash calculated from the vector, which will be required for removal.</returns>
        public ByteString Register(long entryId, ReadOnlySpan<byte> vector)
        {
            entryId = EntryIdToInternalEntryId(entryId);
            PortableExceptions.ThrowIfOnDebug<ArgumentOutOfRangeException>((entryId & Constants.Graphs.VectorId.EnsureIsSingleMask) != 0, "Entry ids must have the first two bits cleared, we are using those");
            PortableExceptions.ThrowIf<ArgumentOutOfRangeException>(
                vector.Length != _searchState.Options.VectorSizeBytes,
                $"Vector size {vector.Length} does not match expected size: {_searchState.Options.VectorSizeBytes}");

            long hashContainerId;
            var hashBuffer = ComputeHashFor(vector);
            ref (ByteString Hash, int NodeIndex, NativeList<long> PostingList) postingList = ref CollectionsMarshal.GetValueRefOrAddDefault(_vectorHashCache, hashBuffer, out var exists);
            if(exists)
            {
                // already added this in the current batch
                ref var l = ref postingList.PostingList;
                l.Add(_searchState.Llt.Allocator, entryId);
                // key already exists in the dictionary, so can clear this 
                var nodeIdExists = _vectorsByHash.TryGetValue(hashBuffer.ToReadOnlySpan(), out hashContainerId, out _);
                _searchState.Llt.Allocator.Release(ref hashBuffer);
                Debug.Assert(nodeIdExists, $"nodeIdExists");
                return postingList.Hash;
            }

            var vectorHash = hashBuffer.ToReadOnlySpan();
            if (_vectorsByHash.TryGetValue(vectorHash, out hashContainerId, out var vectorId) is false)
            {
                vectorId = RegisterVector(vector);
                hashContainerId = _vectorsByHash.Add(vectorHash, vectorId);
            }
            
            if (_nodesByVectorId.TryGetValue(vectorId, out var nodeId))
            {
                int nodeIndex = _searchState.GetNodeIndexById(nodeId);
                postingList = (hashBuffer, nodeIndex, NativeList<long>.Create(_searchState.Llt.Allocator, entryId));
                return hashBuffer;
            }

            long newNodeId = ++_searchState.Options.CountOfVectors;
            int nodeIdx = _searchState.RegisterVectorNode(newNodeId, vectorId);
            _nodesByVectorId.Add(vectorId, newNodeId);
            
            postingList = (hashBuffer, nodeIdx, ToPostingListTuple(entryId));
            return hashBuffer;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        NativeList<long> ToPostingListTuple(long entryId)
        {
            var list = new NativeList<long>();
            list.Add(_searchState.Llt.Allocator, entryId);
            return list;
        }
        
        private long RegisterVector(ReadOnlySpan<byte> vector)
        {
            if (_searchState.Options.LastUsedContainerId is 0)
            {
                if (_vectorBatchSizeInPages is 1)
                {
                    // here we allocate a small value, directly from the container
                    var vectorId = Container.Allocate(_searchState.Llt, _globalVectorsContainerId, 
                        vector.Length, out var singleVectorStorage);

                    vector.CopyTo(singleVectorStorage);
                    return vectorId;
                }

                var sizeInBytes = _vectorBatchSizeInPages * Constants.Storage.PageSize - PageHeader.SizeOf;
                var batchId = Container.Allocate(_searchState.Llt, _globalVectorsContainerId,
                    sizeInBytes, out var vectorStorage);
                
                Debug.Assert(vectorStorage.Length / _searchState.Options.VectorSizeBytes <= byte.MaxValue, "vectorStorage.Length / _searchState.Options.VectorSizeBytes <= byte.MaxValue");
                Debug.Assert((batchId & 0xFFF) == 0, "We allocate > 1 page, so we get the full page container id");
                _searchState.Options.LastUsedContainerId = batchId;
                _searchState.Options.VectorBatchIndex = 1;
                vector.CopyTo(vectorStorage);
                //container id | index    | marker
                return GetVectorId(batchId, 0);
            }
            var span = Container.GetMutable(_searchState.Llt, _searchState.Options.LastUsedContainerId);
            var count = _searchState.Options.VectorBatchIndex++;
            Debug.Assert(((count) * vector.Length) < span.Length, "((count) * vector.Length) < span.Length");
            var offset = count * vector.Length;
            vector.CopyTo(span[offset..]);
            offset += vector.Length;
            var id = GetVectorId(_searchState.Options.LastUsedContainerId, count);
            if (offset + vector.Length > span.Length)
            {
                // no more room for the _next_ vector
                _searchState.Options.LastUsedContainerId = 0;
                _searchState.Options.VectorBatchIndex = 0;
            }
            return id;

            long GetVectorId(long containerId, int index)
            {
                Debug.Assert((containerId & Constants.Graphs.VectorId.EnsureIsSingleMask) == 0, $"Container id {containerId}");
                //container id | index    | marker
                return containerId | (uint)(index << 1) | Constants.Graphs.VectorStorage.VectorContainerInternalIndexer;
            }
        }

        private ByteString ComputeHashFor(ReadOnlySpan<byte> vector)
        {
            _searchState.Llt.Allocator.AllocateDirect(Sodium.GenericHashSize, out var hashBuffer);
            Sodium.GenericHash(vector, hashBuffer.ToSpan());
            return hashBuffer;
        }
        
        private int GetLevelForNewNode(int maxLevel)
        {
            int level = 0;
            while ((Random.Next() & 1) == 0 && // 50% chance 
                   level < maxLevel)
            {
                level++;
            }
            return level;
        }

        public void Commit()
        {
            PortableExceptions.ThrowIfOnDebug<InvalidOperationException>(_searchState.Llt.Committed);
            
            var pforEncoder = new FastPForEncoder(_searchState.Llt.Allocator);
            var pforDecoder = new FastPForDecoder(_searchState.Llt.Allocator);
            var listBuffer = new ContextBoundNativeList<long>(_searchState.Llt.Allocator);
            var byteBuffer = new ContextBoundNativeList<byte>(_searchState.Llt.Allocator);
            byteBuffer.EnsureCapacityFor(128);

            var nodes = _searchState.Nodes;
            foreach (var (_, (_, nodeIndex, modifications)) in _vectorHashCache)
            {
                ref var node = ref nodes[nodeIndex];
                node.PostingListId = MergePostingList(node.PostingListId, modifications);
            }
            
            // Intentionally zeroing the nodes var, we may realloc the underlying array in the insert vector phase
            nodes = Span<Node>.Empty;
            _ = nodes;

            InsertVectorsToGraph(ref byteBuffer);

            nodes = _searchState.Nodes;
            for (int i = 0; i < nodes.Length; i++)
            {
                PersistNode(ref nodes[i], ref byteBuffer);
            }

            // flush the local modifications
            _searchState.FlushOptions();
            
            listBuffer.Dispose();
            byteBuffer.Dispose();
            pforEncoder.Dispose();
            pforDecoder.Dispose();

            IsCommited = true;
            
            long MergePostingList(long postingList, NativeList<long> modifications)
            {
                listBuffer.Clear();
                listBuffer.AddRange(modifications.ToSpan());
                int currentSize = 0;
                bool hasSmallPostingList = false;
                long rawPostingListId = postingList & Constants.Graphs.VectorId.ContainerType;
                switch (postingList & Constants.Graphs.VectorId.EnsureIsSingleMask)
                {
                    case Constants.Graphs.VectorId.Tombstone: // nothing there
                        break;
                    case Constants.Graphs.VectorId.Single: // single value, just add it
                        listBuffer.Add(rawPostingListId);
                        break;
                    case Constants.Graphs.VectorId.SmallPostingList:
                        hasSmallPostingList = true;
                        _searchState.ReadPostingList(rawPostingListId, ref listBuffer, ref pforDecoder, out currentSize);
                        break;
                    case Constants.Graphs.VectorId.PostingList:
                        return UpdatePostingList(rawPostingListId, in modifications, pforEncoder, ref pforDecoder, ref listBuffer);
                }
                
                PostingList.SortEntriesAndRemoveDuplicatesAndRemovals(ref listBuffer);
                if (listBuffer.Count is 0 or 1)
                {
                    if (hasSmallPostingList)
                    {
                        Container.Delete(_searchState.Llt, _searchState.Options.Container, rawPostingListId);
                    }

                    if (listBuffer.Count is 0) 
                        return 0;
                    
                    Debug.Assert((listBuffer[0] & Constants.Graphs.VectorId.PostingList) == 0, "(listBuffer[0] & 0b11) == 0");
                    return listBuffer[0] | Constants.Graphs.VectorId.Single;
                }

                int size = pforEncoder.Encode(listBuffer.RawItems, listBuffer.Count);
                if (size > Container.MaxSizeInsideContainerPage)
                {
                    DeleteOldSmallPostingListIfNeeded();
                    return CreateNewPostingList(pforEncoder);
                }

                byteBuffer.EnsureCapacityFor(size + 5);
                var offset = VariableSizeEncoding.Write(byteBuffer.RawItems, listBuffer.Count);
                (int itemsCount, int sizeUsed) = pforEncoder.Write(byteBuffer.RawItems + offset, byteBuffer.Capacity - offset);
                byteBuffer.Count = sizeUsed + offset;
                Debug.Assert(itemsCount == listBuffer.Count && sizeUsed == size, "itemsCount == listBuffer.Count && sizeUsed == size");
                Span<byte> mutable;
                if (currentSize == byteBuffer.Count)
                {
                    mutable = Container.GetMutable(_searchState.Llt, rawPostingListId);
                }
                else
                {
                    DeleteOldSmallPostingListIfNeeded();
                    rawPostingListId = Container.Allocate(_searchState.Llt, _searchState.Options.Container, byteBuffer.Count, out mutable);
                }
                Span<byte> span = byteBuffer.ToSpan();
                span.CopyTo(mutable);

                Debug.Assert((rawPostingListId & Constants.Graphs.VectorId.PostingList) == 0, "(rawPostingListId & 0b11) == 0");
                return rawPostingListId | Constants.Graphs.VectorId.SmallPostingList;
                
                
                void DeleteOldSmallPostingListIfNeeded()
                {
                    if (hasSmallPostingList)
                    {
                        Container.Delete(_searchState.Llt, _searchState.Options.Container, rawPostingListId);
                    }
                }
            }
        }
        
        public void Dispose()
        {
            //todo: we may wants to release the vector hash cache
        }

        private long UpdatePostingList(long postingListId, in NativeList<long> modifications, FastPForEncoder pForEncoder, ref FastPForDecoder pForDecoder, ref ContextBoundNativeList<long> tempListBuffer)
        {
            var setSpace = Container.GetMutable(_searchState.Llt, postingListId);
            ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace);

            var lists = stackalloc long*[2];
            var indexes = stackalloc int[2];

            using var _1 = _searchState.Llt.Allocator.Allocate(modifications.Count * sizeof(long), out var bs1);
            using var _2 = _searchState.Llt.Allocator.Allocate(modifications.Count * sizeof(long), out var bs2);
            lists[0] = (long*)bs1.Ptr;
            lists[1] = (long*)bs2.Ptr;

            for (int i = 0; i < modifications.Count; i++)
            {
                var cur = modifications[i];
                var listIdx = cur & 1;
                var curIndex = indexes[listIdx]++;
                lists[listIdx][curIndex] = cur;
            }

            var numberOfEntries = PostingList.Update(_searchState.Llt, ref postingListState, lists[0], indexes[0], 
                lists[1], indexes[1], pForEncoder, ref tempListBuffer, ref pForDecoder);

            if(numberOfEntries is 0)
            {
                _largePostingListSet ??= _searchState.Llt.Transaction.OpenPostingList(Constants.PostingList.PostingListRegister);
                _largePostingListSet.Remove(postingListId);
                Container.Delete(_searchState.Llt, _searchState.Options.Container, postingListId);
                return 0;
            }

            return postingListId | Constants.Graphs.VectorId.PostingList;
        }

        private long CreateNewPostingList(FastPForEncoder pforEncoder)
        {
            long setId = Container.Allocate(_searchState.Llt, _searchState.Options.Container, sizeof(PostingListState), out var setSpace);

            _largePostingListSet ??= _searchState.Llt.Transaction.OpenPostingList(Constants.PostingList.PostingListRegister);
            _largePostingListSet.Add(setId);
            
            ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace);
            PostingList.Create(_searchState.Llt, ref postingListState, pforEncoder);
            return setId | Constants.Graphs.VectorId.PostingList;
        }


        void PersistNode(ref Node node, ref ContextBoundNativeList<byte> byteBuffer)
        {
            var encoded = node.Encode(ref byteBuffer);
            if (_searchState.TryGetLocationForNode(node.NodeId, out var locationId))
            {
                var existing = Container.GetMutable(_searchState.Llt, locationId);
                if (existing.Length == encoded.Length)
                {
                    encoded.CopyTo(existing);
                    return;
                }

                Container.Delete(_searchState.Llt, _searchState.Options.Container, locationId);
            }

            locationId = Container.Allocate(_searchState.Llt, _searchState.Options.Container, encoded.Length, out var storage);
            _searchState.RegisterNodeLocation(node.NodeId, locationId);
            encoded.CopyTo(storage);
        }


        void InsertVectorsToGraph(ref ContextBoundNativeList<byte> byteBuffer)
        {
            if (_searchState.TryGetLocationForNode(EntryPointId, out var entryPointNode) is false)
            {
                if (_searchState.CreatedNodesCount == 0)
                    return;

                ref Node startingNode = ref _searchState.Nodes[0];
                Span<byte> span = startingNode.Encode(ref byteBuffer);
                entryPointNode = Container.Allocate(_searchState.Llt, _searchState.Options.Container, span.Length, out Span<byte> allocated);
                span.CopyTo(allocated);
                _searchState.RegisterNodeLocation(EntryPointId, entryPointNode);
            }

            var nearestNodesByLevel = new NativeList<int>();
            var edges = new NativeList<int>();
            var tmp = new NativeList<int>();

            nearestNodesByLevel.EnsureCapacityFor(_searchState.Llt.Allocator, _searchState.Options.MaxLevel + 1);

            for (int currentNodeIndex = 0; currentNodeIndex < _searchState.CreatedNodesCount; currentNodeIndex++)
            {
                nearestNodesByLevel.Clear();

                var currentMaxLevel = _searchState.Options.CurrentMaxLevel(_searchState.CreatedNodesCount - currentNodeIndex);
                int nodeRandomLevel = GetLevelForNewNode(currentMaxLevel);
                Span<byte> vector;
                {
                    // intentionally scoping Node here, to avoid "leaking" the reference
                    // it isn't _stable_ one and may move if the _nodes list is realloced
                    ref var node = ref _searchState.GetNodeByIndex(currentNodeIndex);
                    node.EdgesPerLevel.SetCapacity(_searchState.Llt.Allocator, nodeRandomLevel + 1);
                    vector = node.GetVector(_searchState);
                    _searchState.SearchNearestAcrossLevels(vector, currentNodeIndex, currentMaxLevel, ref nearestNodesByLevel);
                }
                for (int level = nodeRandomLevel; level >= 0; level--)
                {
                    int startingPointIndex = nearestNodesByLevel[level];
                    edges.Clear();
                    var flags = currentNodeIndex != startingPointIndex ? 
                        SearchState.NearestEdgesFlags.StartingPointAsEdge : 
                        SearchState.NearestEdgesFlags.None;
                    
                    _searchState.NearestEdges(startingPointIndex, currentNodeIndex,
                        vector,
                        level, _searchState.Options.NumberOfCandidates, ref edges, flags);

                    if (edges.Count > _searchState.Options.NumberOfEdges)
                        _searchState.FilterEdgesHeuristic(currentNodeIndex, ref edges);

                    ref var node = ref _searchState.GetNodeByIndex(currentNodeIndex);
                    ref var list = ref node.EdgesPerLevel[level];
                    list.EnsureCapacityFor(_searchState.Llt.Allocator, edges.Count);
                    list.Clear();
                    for (int i = 0; i < edges.Count; i++)
                    {
                        int edgeIdx = edges[i];
                        ref Node edge = ref _searchState.GetNodeByIndex(edgeIdx);
                        list.AddUnsafe(edge.NodeId);
                        Debug.Assert(edge.NodeId != node.NodeId, "edge.NodeId != node.NodeId");
                        
                        ref var edgeList = ref edge.EdgesPerLevel[level];
                        edgeList.Add(_searchState.Llt.Allocator, node.NodeId);

                        if (edgeList.Count <= _searchState.Options.NumberOfEdges)
                            continue;

                        // FilterEdgesHeuristic works on node indexes, while edges list is node ids
                        // so we need to convert them back & forth in this manner
                        tmp.ResetAndEnsureCapacity(_searchState.Llt.Allocator, edgeList.Count);
                        for (int k = 0; k < edgeList.Count; k++)
                        {
                            tmp.AddUnsafe(_searchState.GetNodeIndexById(edgeList[k]));
                        }
                        _searchState.FilterEdgesHeuristic(edgeIdx, ref tmp);
                        edgeList.Clear();
                        for (int k = 0; k < tmp.Count; k++)
                        {
                            edgeList.AddUnsafe(_searchState.GetNodeByIndex(tmp[k]).NodeId);
                        }
                    }
                }
            }
        }
    }

    public static Registration RegistrationFor(LowLevelTransaction llt, string name, Random random = null)
    {
        Slice.From(llt.Allocator, name, out var slice);
        return RegistrationFor(llt, slice, random);
    }
    public static Registration RegistrationFor(LowLevelTransaction llt, Slice name, Random random = null)
    {
        return new Registration(llt, name, random);
    }

    public static NearestSearch ExactNearest(LowLevelTransaction llt, string name, int numberOfCandidates, ReadOnlySpan<byte> vector, float minimumSimilarity)
    {
        Slice.From(llt.Allocator, name, out var slice);
        return ExactNearest(llt, slice, numberOfCandidates, vector, minimumSimilarity);
    }

    public static NearestSearch ExactNearest(LowLevelTransaction llt, Slice name, int numberOfCandidates, ReadOnlySpan<byte> vector, float minimumSimilarity)
    {
        var searchState = new SearchState(llt, name);
        var pq = new PriorityQueue<long, float>();
        for (long nodeId = 1; nodeId <= searchState.Options.CountOfVectors; nodeId++)
        {
            searchState.ReadNode(nodeId, out var reader);
            if (reader.PostingListId is 0)
                continue; // no entries, can skip

            var curVect = reader.ReadVector(in searchState);
            var distance = searchState.SimilarityCalc(vector, curVect);
            if (pq.Count < numberOfCandidates)
            {
                pq.Enqueue(nodeId, -distance);
            }
            else
            {
                pq.EnqueueDequeue(nodeId, -distance);
            }
        }

        var candidates = new ContextBoundNativeList<int>(llt.Allocator);
        while(pq.TryDequeue(out var nodeId, out _))
        {
            var nodeIdx = searchState.GetNodeIndexById(nodeId);
            candidates.Add(nodeIdx);
        }
        candidates.Inner.Reverse();
        return new NearestSearch(searchState, candidates, vector, minimumSimilarity);
    }

    public static NearestSearch ApproximateNearest(LowLevelTransaction llt, string name, int numberOfCandidates, ReadOnlySpan<byte> vector, float minimumSimilarity)
    {
        Slice.From(llt.Allocator, name, out var slice);
        return ApproximateNearest(llt, slice, numberOfCandidates, vector, minimumSimilarity);
    }

    public static NearestSearch ApproximateNearest(LowLevelTransaction llt, Slice name, int numberOfCandidates, ReadOnlySpan<byte> vector, float minimumSimilarity)
    {
        var searchState = new SearchState(llt, name);
        var nearestNodesByLevel = new ContextBoundNativeList<int>(llt.Allocator);
        nearestNodesByLevel.EnsureCapacityFor(searchState.Options.MaxLevel + 1);

        if (searchState.Options.CountOfVectors == 0)
            return new NearestSearch(searchState, nearestNodesByLevel, vector, minimumSimilarity);
        
        searchState.SearchNearestAcrossLevels(vector, -1, searchState.Options.MaxLevel, ref nearestNodesByLevel.Inner);
        var nearest = nearestNodesByLevel[0];
        nearestNodesByLevel.Clear();
        searchState.NearestEdges(nearest, -1, vector, level: 0, numberOfCandidates: numberOfCandidates, candidates: ref nearestNodesByLevel.Inner, 
            SearchState.NearestEdgesFlags.StartingPointAsEdge | SearchState.NearestEdgesFlags.FilterNodesWithEmptyPostingLists);
        return new NearestSearch(searchState, nearestNodesByLevel, vector, minimumSimilarity);
    }

    public struct NearestSearch : IDisposable
    {
        public NearestSearch(SearchState searchState, ContextBoundNativeList<int> indexes, ReadOnlySpan<byte> vector, float minimumSimilarity)
        {
            _searchState = searchState;
            _indexes = indexes;
            _postingListResults = new(_searchState.Llt.Allocator);
            _pforDecoder = new(searchState.Llt.Allocator);
            searchState.Llt.Allocator.AllocateDirect(vector.Length, out _vector);
            vector.CopyTo(_vector.ToSpan());
            _maximumDistance = searchState.MinimumSimilarityToDistance(minimumSimilarity);
        }

        private ContextBoundNativeList<int> _indexes;
        private SearchState _searchState;
        private int _currentNode, _currentMatchesIndex;
        private ContextBoundNativeList<long> _postingListResults;
        private FastPForDecoder _pforDecoder;
        private ByteString _vector;
        private PostingList.Iterator _postingListIterator;
        private PostingList _postingList;
        private readonly float _maximumDistance;
        public SimilarityMethod SimilarityMethod => _searchState.Options.SimilarityMethod;
        public bool IsEmpty => _searchState.IsEmpty;

        public void Dispose()
        {
            _indexes.Dispose();
            _postingListResults.Dispose();
            _pforDecoder.Dispose();
            _searchState.Llt.Allocator.Release(ref _vector);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float DistanceToScore(float distance) => _searchState.DistanceToScore(distance);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DistancesToScores(Span<float> distances) => _searchState.DistancesToScores(distances);
        
        public int Fill(Span<long> matches, Span<float> distances)
        {
            int index = 0;
            float distance = float.NaN;
            while (index < matches.Length)
            {
                if (_currentNode >= _indexes.Count)
                    break;

                if(_postingList != null)
                {
                    if (_postingListIterator.Fill(matches[index..], out var total) is false && total is 0)
                    {
                        _postingListIterator = default;
                        _postingList = null;
                        _currentNode++;
                        continue;
                    }
                    distances.Slice(index, total).Fill(distance);
                    index += total;
                    continue;
                }

                if (_currentMatchesIndex < _postingListResults.Count)
                {
                    var copy = Math.Min(_postingListResults.Count - _currentMatchesIndex, matches.Length - index);
                    _postingListResults.CopyTo(matches[index..], _currentMatchesIndex, copy);
                    distances.Slice(index, copy).Fill(distance);
                    index += copy;
                    _currentMatchesIndex += copy;
                    if(_currentMatchesIndex == _postingListResults.Count)
                    {
                        _currentMatchesIndex = 0;
                        _postingListResults.Clear();
                        _currentNode++;
                    }
                    continue;
                }

                var nodeIdx = _indexes[_currentNode];
                ref var node = ref _searchState.GetNodeByIndex(nodeIdx);
                var rawPostingListId = node.PostingListId & Constants.Graphs.VectorId.ContainerType;
                distance = _searchState.Distance(_vector.ToSpan(), -1, nodeIdx);

                if (distance > _maximumDistance)
                {
                    _currentNode++;
                    continue;
                }
                
                switch (node.PostingListId & Constants.Graphs.VectorId.EnsureIsSingleMask)
                {
                    case Constants.Graphs.VectorId.Tombstone: // empty
                        _currentNode++;
                        continue;
                    case Constants.Graphs.VectorId.Single: // single item posting list
                        distances[index] = distance;
                        matches[index++] = rawPostingListId;
                        _currentNode++;
                        continue;
                    case Constants.Graphs.VectorId.SmallPostingList: // small posting list
                        Debug.Assert(_postingListResults.Count is 0 && _currentMatchesIndex is 0);
                        _searchState.ReadPostingList(rawPostingListId, ref _postingListResults, ref _pforDecoder, out _);
                        continue;
                    case Constants.Graphs.VectorId.PostingList: // large posting list
                        var setStateSpan = Container.GetReadOnly(_searchState.Llt, rawPostingListId);
                        ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
                        _postingList = new PostingList(_searchState.Llt, Slices.Empty, setState);
                        _postingListIterator = _postingList.Iterate();
                        continue;
                    default:
                        throw new ArgumentOutOfRangeException("Impossible scenario, we have only 4 options, but got: " + node.PostingListId);
                }
            }

            Registration.InternalEntryIdToEntryId(matches.Slice(0, index));
            return index;
        }
    }
}
