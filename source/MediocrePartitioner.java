package testinggg;
import java.util.ArrayList;
import java.util.Random;

/**  MediocrePartitioner splits the given matrix into sideS x sideR rectangles , one for each reducer and assigns given tuples to them. 
 *
 * 
 */
public class MediocrePartitioner {

    private int S;              //Number of S tuples
    private int R;              //Number of R tuples
    private int r;              //Number of reducers
    private double sideS;       //one side of the rectangle
    private double sideR;       // the other side 
    private int blocksS;        // number of vertical rectangles
    private int blocksR;        // number of horizontal rectangles
    private int[][] myRegions;  // table with the assigned regions

//Constructor
    public MediocrePartitioner(int S, int R, int r) {

        this.S = S;
        this.R = R;
        this.r = r;
        computePartitions();
    }
/**Returns a List of regions depending on a random value.
 * 
 * @param str                               tuple identifier "R" for R and "S" for S
 * @return ArrayList<Integer> myList        a list of regions
 */
    public ArrayList getRegions(String str) {
        Random rn = new Random();
        int randNumber = 0;
        ArrayList myList = new ArrayList<Integer>();
        // for an R tuple produce a random number and locate the column of rectanles it is assigned to
        if (str.equals("R")) {
            randNumber = rn.nextInt(R);
            int regionCol = 0;
            for (int i = 1; i < blocksR; i++) {
                if (randNumber < i * sideR) {
                    regionCol = i;
                    break;
                }
            }
            for (int i = 0; i < blocksS; i++) {
                myList.add(myRegions[i][regionCol]);
            }
        // for an S tuple produce a random number and locate the row of rectanlges it is assigned to
        } else if (str.equals("S")) {
            randNumber = rn.nextInt(S);
            int regionRow = 0;
            for (int i = 1; i < blocksS; i++) {
                if (randNumber < i * sideS) {
                    regionRow = i;
                    break;
                }
            }
            for (int i = 0; i < blocksR; i++) {
                myList.add(myRegions[regionRow][i]);
            }
        }
        return myList; // return a list with all the reducers the tuple was assigned to
    }
    /**computePartitions finds the optimal partition square for a SxR matrix and inflates or deflates each side to achive 
     * a rectangle that contains smaller reducerNum*rectangles that match the SxR matrix
     * 
     */
    public void computePartitions() {

        double side = Math.sqrt((double) (S * R) / r);
        double mysideS = side;
        double mysideR = side;
        
        //if if the side is bigger than |S| or |R|, partition the matrix in columns or stripes. Else compute the rectangles and inflate/deflate them.
        if (mysideS > S) {
            mysideS = S;
            mysideR = (double) R / r;
        } else if (mysideR > R) {
            mysideR = R;
            mysideS = (double) S / r;
        } else {
            if (S < R) {        //if S<R find how many optimal squares fit in a S side of the matrix and compute the rest for R
                int counterS = 0;
                for (int i = 0; i < S; i++) {
                    if ((counterS + 1) * mysideS > S) {
                        break;
                    }
                    counterS++;
                }
                blocksS = counterS;  //number of optimal squares that fit in the S side.
                int counterR;
                counterR = r / counterS;
                blocksR = counterR; //number of squares we need to put in the R side in order to achieve r reducers.
                if (counterR * counterS != r) {
                    counterR = counterR + (r - counterR * counterS); // reduce the number of reducers if we cant get a AxB rectangle of rectangles
                }

                if (counterR * mysideR > R) {
                    //deflate Rside of the rectangle
                    double diaforaR = (counterR * mysideR - R) / r;
                    mysideR = mysideR - diaforaR;

                } else {
                    //inflate R side of the rectangle

                    double diaforaR = (R - counterR * mysideR) / counterS;
                    mysideR = mysideR + diaforaR;

                }
                //inflate S side of the rectangle
                double diaforaS = (S - counterS * mysideS) / counterS;
                mysideS = mysideS + diaforaS;

            } else { //if R<S find how many optimal squares fit in a R side of the matrix and compute the rest for S
                int counterR = 0;
                for (int i = 0; i < R; i++) {
                    if ((counterR + 1) * mysideR > R) {
                        break;
                    }
                    counterR++;
                }
                blocksR = counterR; //number of optimal squares that fit on the R side of the matrix
                int counterS;
                counterS = r / counterR;
                blocksS = counterS; //number of squares we need to put in the S side in order to achieve r reducers.
                if (counterS * counterR != r) {
                    counterS = counterS + (r - counterS * counterR);  // reduce the number of reducers if we cant get a AxB rectangle of rectangles
                }
                if (counterS * mysideS > S) {
                    //deflate S side of rectangles
                    double diaforaS = (counterS * mysideS - S) / r;
                    mysideS = mysideS - diaforaS;
                } else {
                    //inflate S side of rectangles
                    double diaforaS = (S - counterS * mysideS) / counterS;
                    mysideS = mysideS + diaforaS;
                }
                //inflate R side of the rectangles
                double diaforaR = (R - counterR * mysideR) / counterR;
                mysideR = mysideR + diaforaR;
            }
        }
        // create the Lookup table of regions and assign a reducer to each cell 
        sideR = mysideR;
        sideS = mysideS;
        int redCounter = 1;
        myRegions = new int[blocksS][blocksR];
        for (int i = 0; i < blocksS; i++) {
            for (int j = 0; j < blocksR; j++) {
                myRegions[i][j] = redCounter;
                redCounter++;
            }
        }
    }
}
