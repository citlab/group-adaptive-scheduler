class JobGroupData:
    group1 = ["SVM", "LogisticRegression"]
    group2 = ["WordCount", "KMeans"]
    group3 = ["TPCH21"]
    groups = [group1, group2, group3]

    groupIndexes = {"SVM": 0, "LogisticRegression": 0, "WordCount": 1, "KMeans": 1, "TPCH21": 2}

    def get_group_name(self, group_index):
        return self.groups[group_index].__str__()

    # Cluster slots
    SLOT_1 = "slot1"
    SLOT_2 = "slot2"

    cluster_slots_index = {"wally081.cit.tu-berlin.de": SLOT_1,
                           "wally082.cit.tu-berlin.de": SLOT_1,
                           "wally083.cit.tu-berlin.de": SLOT_1,
                           "wally084.cit.tu-berlin.de": SLOT_1,
                           "wally085.cit.tu-berlin.de": SLOT_2,
                           "wally086.cit.tu-berlin.de": SLOT_2,
                           "wally087.cit.tu-berlin.de": SLOT_2,
                           "wally088.cit.tu-berlin.de": SLOT_2, }
