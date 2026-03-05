import { describe, it, expect } from "vitest";
import { makeNode, nodeAddTopic, nodeFindBySubjectId, topicHash, subjectId } from "../../src/sim.js";
import { GOSSIP_DEDUP_CAP, GOSSIP_PEER_COUNT, SUBJECT_ID_MODULUS } from "../../src/constants.js";
import type { Topic } from "../../src/types.js";

function makeTopic(name: string): Topic {
  const hash = topicHash(name);
  return { name, hash, evictions: 0, tsCreatedUs: 0, sortOrder: 0 };
}

describe("makeNode", () => {
  it("has correct defaults", () => {
    const node = makeNode(7);
    expect(node.nodeId).toBe(7);
    expect(node.online).toBe(false);
    expect(node.topics.size).toBe(0);
    expect(node.dedup.length).toBe(GOSSIP_DEDUP_CAP);
    expect(node.peers.length).toBe(GOSSIP_PEER_COUNT);
    expect(node.peers.every(p => p === null)).toBe(true);
    expect(node.partitionSet).toBe("A");
  });
});

describe("nodeAddTopic", () => {
  it("adds to topics map and gossipQueue", () => {
    const node = makeNode(0);
    const topic = makeTopic("topic/a");
    nodeAddTopic(node, topic);
    expect(node.topics.get(topic.hash)).toBe(topic);
    expect(node.gossipQueue).toContain(topic.hash);
  });

  it("does not add duplicates to gossipQueue", () => {
    const node = makeNode(0);
    const topic = makeTopic("topic/a");
    nodeAddTopic(node, topic);
    nodeAddTopic(node, topic);
    expect(node.gossipQueue.filter(h => h === topic.hash).length).toBe(1);
  });
});

describe("nodeFindBySubjectId", () => {
  it("returns null on empty node", () => {
    const node = makeNode(0);
    expect(nodeFindBySubjectId(node, 100)).toBeNull();
  });

  it("finds matching topic", () => {
    const node = makeNode(0);
    const topic = makeTopic("topic/a");
    nodeAddTopic(node, topic);
    const sid = subjectId(topic.hash, topic.evictions, SUBJECT_ID_MODULUS);
    expect(nodeFindBySubjectId(node, sid)).toBe(topic);
  });

  it("returns null when no match", () => {
    const node = makeNode(0);
    const topic = makeTopic("topic/a");
    nodeAddTopic(node, topic);
    expect(nodeFindBySubjectId(node, 999999)).toBeNull();
  });
});
