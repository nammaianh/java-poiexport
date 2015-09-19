package poiexport.callable;

import java.io.IOException;

/**
 * Created by developer on 12/09/2015.
 */
public class InsertionStatusPrinter implements Runnable {

    private int total;
    private int doneCount;
    private float percent;

    public InsertionStatusPrinter(int total, int doneCount) {
        this.total = total;
        this.doneCount = doneCount;
        this.percent = (float) doneCount / total * 100;
    }

    @Override
    public void run() {
        try {
            // Clear console
            Runtime.getRuntime().exec("clear");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        System.out.printf(
                "%d/%d (%f%%)\n",
                this.doneCount,
                this.total,
                this.percent
        );
    }
}
