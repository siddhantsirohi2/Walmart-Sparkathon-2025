# producer/apriori_producer.py
from collections import defaultdict
from itertools import combinations
from kafka import KafkaProducer
import json
import time

def load_dataset():
    """
    Creates a sample grocery transaction dataset.
    Each inner list represents a single transaction.
    """
    return [
        ['Milk', 'Bread', 'Eggs', 'Sugar', 'Butter'],
        ['Bread', 'Butter', 'Cheese', 'Yogurt'],
        ['Milk', 'Diapers', 'Beer', 'Eggs'],
        ['Bread', 'Sugar', 'Jam', 'Juice'],
        ['Milk', 'Bread', 'Butter', 'Cheese'],
        ['Yogurt', 'Juice', 'Sugar'],
        ['Milk', 'Bread', 'Diapers', 'Beer', 'Juice'],
        ['Bread', 'Butter', 'Eggs'],
        ['Milk', 'Cheese', 'Yogurt', 'Sugar'],
        ['Diapers', 'Beer', 'Juice'],
        ['Milk', 'Bread', 'Butter', 'Eggs', 'Cheese'],
        ['Bread', 'Jam', 'Sugar'],
        ['Milk', 'Yogurt', 'Juice'],
        ['Beer', 'Diapers', 'Milk'],
        ['Bread', 'Butter', 'Sugar', 'Jam'],
        ['Milk', 'Eggs', 'Bread', 'Butter', 'Yogurt'],
        ['Cheese', 'Yogurt', 'Juice'],
        ['Milk', 'Diapers', 'Beer', 'Bread'],
        ['Sugar', 'Jam', 'Juice'],
        ['Bread', 'Butter', 'Milk'],
        ['Eggs', 'Cheese', 'Yogurt'],
        ['Milk', 'Beer', 'Diapers'],
        ['Bread', 'Sugar', 'Butter'],
        ['Milk', 'Juice', 'Yogurt', 'Cheese'],
        ['Bread', 'Eggs', 'Jam'],
        ['Milk', 'Bread', 'Butter'],
        ['Diapers', 'Beer', 'Yogurt'],
        ['Bread', 'Cheese', 'Juice'],
        ['Milk', 'Eggs', 'Sugar'],
        ['Butter', 'Jam', 'Bread']
    ]

def create_c1(dataset):
    """
    Create a list of candidate itemsets of size 1.
    """
    c1 = []
    for transaction in dataset:
        for item in transaction:
            if not [item] in c1:
                c1.append([item])
    c1.sort()
    return list(map(frozenset, c1))

def scan_dataset(dataset, candidates, min_support):
    """
    Calculates the support for each candidate itemset and prunes those
    that don't meet the minimum support threshold.
    """
    item_counts = defaultdict(int)
    for transaction in dataset:
        transaction_set = set(transaction)
        for candidate in candidates:
            if candidate.issubset(transaction_set):
                item_counts[candidate] += 1

    num_transactions = float(len(dataset))
    frequent_itemsets = []
    support_data = {}

    for item, count in item_counts.items():
        support = count / num_transactions
        if support >= min_support:
            frequent_itemsets.insert(0, item)
        support_data[item] = support

    return frequent_itemsets, support_data

def generate_candidates(frequent_itemsets_k_minus_1, k):
    """
    Generate candidate itemsets of size k from frequent itemsets of size k-1.
    This is the "join" step of the Apriori algorithm.
    """
    candidates_k = []
    len_lk_minus_1 = len(frequent_itemsets_k_minus_1)

    for i in range(len_lk_minus_1):
        for j in range(i + 1, len_lk_minus_1):
            s1 = list(frequent_itemsets_k_minus_1[i])
            s2 = list(frequent_itemsets_k_minus_1[j])
            s1.sort()
            s2.sort()
            
            if s1[:-1] == s2[:-1]:
                candidates_k.append(frequent_itemsets_k_minus_1[i] | frequent_itemsets_k_minus_1[j])

    return candidates_k

def apriori(dataset, min_support=0.2):
    """
    The main Apriori algorithm function.
    """
    c1 = create_c1(dataset)
    l1, support_data = scan_dataset(dataset, c1, min_support)

    L = [l1]
    k = 2

    while (len(L[k-2]) > 0):
        ck = generate_candidates(L[k-2], k)
        lk, support_k = scan_dataset(dataset, ck, min_support)
        support_data.update(support_k)
        L.append(lk)
        k += 1

    return L, support_data

def generate_rules(L, support_data, min_confidence=0.6):
    """
    Generate association rules from the list of frequent itemsets.
    """
    rules = []
    for i in range(1, len(L)):
        for freq_set in L[i]:
            for h in [frozenset(s) for s in combinations(freq_set, len(freq_set)-1) if len(s) > 0]:
                if h in support_data:
                    confidence = support_data[freq_set] / support_data[h]

                    if confidence >= min_confidence:
                        antecedent = h
                        consequent = freq_set - h
                        support = support_data[freq_set]
                        rule = (antecedent, consequent, confidence, support)
                        rules.append(rule)
    return rules

def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'], # Use localhost when running from host
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

if __name__ == '__main__':
    MIN_SUPPORT = 0.20
    MIN_CONFIDENCE = 0.7

    dataset = load_dataset()

    print("Running Apriori algorithm...")
    frequent_itemsets_list, support_data_map = apriori(dataset, min_support=MIN_SUPPORT)
    print("Apriori algorithm finished.")

    association_rules = generate_rules(frequent_itemsets_list, support_data_map, min_confidence=MIN_CONFIDENCE)

    producer = get_kafka_producer()
    topic_name = 'apriori-rules-output'

    print(f"\nSending {len(association_rules)} rules to Kafka topic: {topic_name}")
    for rule in association_rules:
        antecedent = list(rule[0])
        consequent = list(rule[1])
        confidence = round(rule[2], 4)
        support = round(rule[3], 4)

        message = {
            "antecedent": antecedent,
            "consequent": consequent,
            "confidence": confidence,
            "support": support
        }
        producer.send(topic_name, value=message)
        print(f"  Sent: {message}")
        time.sleep(0.1)

    producer.flush()
    print("All rules sent to Kafka. Producer closing.")