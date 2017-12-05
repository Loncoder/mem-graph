import ict.ada.gdb.GolaxyGraph;
import ict.ada.gdb.imodel.Graph;
import ict.ada.gdb.imodel.Node;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class TestCase {
    static Graph graph = GolaxyGraph.createGraph("test1", new Properties());
    static Random random = new Random();

    public static void main(String[] args) {
        int times = 10000 * 10;
        long beginTime = new Date().getTime();
        for (int i = 0; i < times; i++) {
            //Node node = graph.newNode("nt", "id_" + i, 100);
            //Node node1 = graph.newNode("nt", "id_" + (i+10), 100);
            //graph.getNode("nt","id_"+i);
            //graph.newRelation("nt","id_"+i,"nt","id_"+(random.nextInt()%times),"rt",10);
            //node.neighbors();
            //graph.shortestPath(node.gdbNode(),node.gdbNode());
            //graph.getNodesInTwoLevel(node.gdbNode());
        }
        long endTime = new Date().getTime();
        System.out.println("through time : " + (endTime-beginTime));
    }


}
