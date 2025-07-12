package com.example.communitysearch.service.algorithm.impl;

import com.example.communitysearch.model.dto.CommunityResult;
import com.example.communitysearch.model.graph.*;
import com.example.communitysearch.repository.neo4j.VertexRepository;
import com.example.communitysearch.service.algorithm.*;
import com.example.communitysearch.util.Neo4jUtil;
import org.neo4j.driver.*;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class CommunitySearchServiceImpl implements CommunitySearchService {

    private final Neo4jUtil neo4jUtil;
    private final Driver neo4jDriver;
    private final PreprocessService preprocessService;
    private final Greedy greedyAlgorithm;
    private VertexRepository vertexRepository;

    @Autowired
    public CommunitySearchServiceImpl(Neo4jUtil neo4jUtil,
                                      Driver neo4jDriver,
                                      PreprocessService preprocessService,
                                      Greedy greedyAlgorithm) {
        this.neo4jUtil = neo4jUtil;
        this.neo4jDriver = neo4jDriver;
        this.preprocessService = preprocessService;
        this.greedyAlgorithm = greedyAlgorithm;
    }

    /**
     * 根据id，找到neo4j数据库中的值
     * @param k
     * @param d
     * @param nodeId
     * @return
     */
    @Override
    public CommunityResult searchCommunity(int k, int d, Long nodeId) {
        long startTime = System.currentTimeMillis();

        Graph graph = buildGraphFromNeo4j();
        Vertex queryVertex = graph.getVertexMap().get(nodeId);
        if (queryVertex == null) {
            throw new IllegalArgumentException("Node with ID " + nodeId + " not found");
        }

        // 执行预处理
        preprocessService.kdTrussMaintain(graph, queryVertex, k, d);

        // 执行社区搜索
        Graph community = greedyAlgorithm.greedyWeight(graph, graph, k, d, queryVertex);

        return calculateMetrics(community, queryVertex, startTime, nodeId);
    }

    @Override
    public Graph buildGraphFromNeo4j() {
        Graph graph = new Graph();
        try (Session session = neo4jDriver.session()) {
            Result result = session.run("MATCH (n)-[r]->(m) RETURN n, r, m");
            while (result.hasNext()) {
                org.neo4j.driver.Record record = result.next();
                processGraphRecord(record, graph);
            }
        }
        return graph;
    }

    private CommunityResult calculateMetrics(Graph community, Vertex queryVertex, long startTime, Long queryNodeId) {
        CommunityResult result = new CommunityResult();

        // 1. 基础指标
        result.setCommunitySize(community.getVertices().size());
        result.setResponseTime(System.currentTimeMillis() - startTime);
        result.setQueryNodeId(queryNodeId); // 设置查询节点ID

        // 2. 结构紧密度 = 社区内部边权重之和 / 总边权重
        double internalWeight = community.getEdges().stream()
                .mapToDouble(Edge::getWeight)
                .sum();
        result.setStructuralTightness(internalWeight); // 对于子图，总权重就是内部权重

        // 3. 属性相似度指标（Jaccard和Cosine）
        if (queryVertex.getAttributes() != null && !queryVertex.getAttributes().isEmpty()) {
            List<Integer> queryAttrs = queryVertex.getAttributes();
            double jaccardSum = 0;
            double cosineSum = 0;
            int count = 0;

            for (Vertex v : community.getVertices()) {
                if (v.getAttributes() != null && !v.getAttributes().isEmpty()) {
                    jaccardSum += calculateJaccardSimilarity(queryAttrs, v.getAttributes());
                    cosineSum += calculateCosineSimilarity(queryAttrs, v.getAttributes());
                    count++;
                }
            }

            if (count > 0) {
                result.setJaccardSimilarity(jaccardSum / count);
                result.setCosineSimilarity(cosineSum / count);
            }
        }

        // 4. 中心性指标（查询节点的度中心性）
        if (community.getVertices().size() > 1) {
            int degree = community.getVertexMap().get(queryVertex.getId()).getNeighbors().size();
            result.setCentrality((double) degree / (community.getVertices().size() - 1));
        }

        // 5. 可视化数据结构
        result.setNodes(community.getVertices().stream()
                .map(v -> {
                    Map<String, Object> nodeData = new HashMap<>();
                    nodeData.put("id", v.getId());
                    nodeData.put("type", v.getType());
                    nodeData.put("attributes", v.getAttributes());
                    nodeData.put("isQueryNode", v.getId().equals(queryNodeId));
                    return nodeData;
                })
                .collect(Collectors.toList()));

        result.setEdges(community.getEdges().stream()
                .map(e -> {
                    Map<String, Object> edgeData = new HashMap<>();
                    edgeData.put("id", e.getId());
                    edgeData.put("source", e.getSource().getId());
                    edgeData.put("target", e.getTarget().getId());
                    edgeData.put("type", e.getType());
                    edgeData.put("weight", e.getWeight());
                    return edgeData;
                })
                .collect(Collectors.toList()));

        return result;
    }

    private double calculateJaccardSimilarity(List<Integer> attrs1, List<Integer> attrs2) {
        Set<Integer> intersection = new HashSet<>(attrs1);
        intersection.retainAll(attrs2);

        Set<Integer> union = new HashSet<>(attrs1);
        union.addAll(attrs2);

        return union.isEmpty() ? 0 : (double) intersection.size() / union.size();
    }

    private double calculateCosineSimilarity(List<Integer> attrs1, List<Integer> attrs2) {
        Set<Integer> allAttrs = new HashSet<>(attrs1);
        allAttrs.addAll(attrs2);

        double dotProduct = 0;
        double normA = 0;
        double normB = 0;

        for (Integer attr : allAttrs) {
            int a = attrs1.contains(attr) ? 1 : 0;
            int b = attrs2.contains(attr) ? 1 : 0;
            dotProduct += a * b;
            normA += a * a;
            normB += b * b;
        }

        return (normA == 0 || normB == 0) ? 0 :
                dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    private void processGraphRecord(org.neo4j.driver.Record record, Graph graph) {
        Node neo4jNode = record.get("n").asNode();
        processNode(neo4jNode, graph);

        Relationship neo4jRel = record.get("r").asRelationship();
        Node targetNode = record.get("m").asNode();
        processNode(targetNode, graph);

        processRelationship(neo4jRel, graph);
    }

    private void processNode(Node node, Graph graph) {
        Long nodeId = Long.parseLong(String.valueOf(node.id()));
        Vertex vertex = new Vertex(nodeId);
        if (node.hasLabel("User")) {
            vertex.setType(1);
        } else if (node.hasLabel("Post")) {
            vertex.setType(2);
        }
        List<Integer> attributes = new ArrayList<>();
        node.asMap().forEach((key, value) -> {
            if (value instanceof Number) {
                attributes.add(((Number) value).intValue());
            }
        });
        vertex.setAttributes(attributes);
        graph.addVertex(vertex);
    }

    private void processRelationship(Relationship rel, Graph graph) {
        Long edgeId = Long.parseLong(String.valueOf(rel.id()));
        Edge edge = new Edge(edgeId);
        if (rel.hasType("FOLLOWS")) {
            edge.setType(1);
        } else if (rel.hasType("LIKES")) {
            edge.setType(2);
        }
        graph.addEdge(edge,
//                graph.getVertexById(Long.parseLong(rel.startNodeElementId())),
//                graph.getVertexById(Long.parseLong(rel.startNodeElementId())),
                graph.getVertexById(Long.parseLong(String.valueOf(rel.startNodeId()))),
                graph.getVertexById(Long.parseLong(String.valueOf(rel.endNodeId()))));
    }
}