package demopackage;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

class Reduce extends Reducer<LongWritable, FriendCountWritable, LongWritable, Text> {
    @Override
    public void reduce(LongWritable key, Iterable<FriendCountWritable> values, Context context)
            throws IOException, InterruptedException {

        // key is the recommended friend, and value is the list of mutual friends
        final java.util.Map<Long, List<Long>> mutualFriends = new HashMap<Long, List<Long>>();

        for (FriendCountWritable val : values) {
            final Boolean isAlreadyFriend = (val.mutualFriend == -1);
            final Long toUser = val.user;
            final Long mutualFriend = val.mutualFriend;

            if (mutualFriends.containsKey(toUser)) {
                if (isAlreadyFriend) {
                    mutualFriends.put(toUser, null);
                } else if (mutualFriends.get(toUser) != null) {
                    mutualFriends.get(toUser).add(mutualFriend);
                }
            } else {
                if (!isAlreadyFriend) {
                    mutualFriends.put(toUser, new ArrayList<Long>() {
                        {
                            add(mutualFriend);
                        }
                    });
                } else {
                    mutualFriends.put(toUser, null);
                }
            }
        }

        java.util.SortedMap<Long, List<Long>> sortedMutualFriends = new TreeMap<Long, List<Long>>(new Comparator<Long>() {
            @Override
            public int compare(Long key1, Long key2) {
                Integer v1 = mutualFriends.get(key1).size();
                Integer v2 = mutualFriends.get(key2).size();
                if (v1 > v2) {
                    return -1;
                } else if (v1.equals(v2) && key1 < key2) {
                    return -1;
                } else {
                    return 1;
                }
            }
        });

        for (java.util.Map.Entry<Long, List<Long>> entry : mutualFriends.entrySet()) {
            if (entry.getValue() != null) {
                sortedMutualFriends.put(entry.getKey(), entry.getValue());
            }
        }

        Integer i = 0;
        String output = "";
        for (java.util.Map.Entry<Long, List<Long>> entry : sortedMutualFriends.entrySet()) {
            if (i == 0) {
                output = entry.getKey().toString() + " (" + entry.getValue().size() + ": " + entry.getValue() + ")";
            } else {
                output += "," + entry.getKey().toString() + " (" + entry.getValue().size() + ": " + entry.getValue() + ")";
            }
            ++i;
        }
        context.write(key, new Text(output));
    }
}
