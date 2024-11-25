package Utilities;

import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;

/**
 * Calculates parameter eta for GSP using bisection
 */
public class EtaFunc {

    // Flag to determine whether to use approximate eta calculation
    private static final boolean APPROX_ETA = false;

    // Precomputed weights for numerical integration
    private static final double[] WEX = new double[32];

    // Gauss-Hermite quadrature weights
    private static final double[] W = {.10921834195238497114,
            .21044310793881323294,
            .23521322966984800539,
            .19590333597288104341,
            .12998378628607176061,
            .70578623865717441560E-1,
            .31760912509175070306E-1,
            .11918214834838557057E-1,
            .37388162946115247897E-2,
            .98080330661495513223E-3,
            .21486491880136418802E-3,
            .39203419679879472043E-4,
            .59345416128686328784E-5,
            .74164045786675522191E-6,
            .76045678791207814811E-7,
            .63506022266258067424E-8,
            .42813829710409288788E-9,
            .23058994918913360793E-10,
            .97993792887270940633E-12,
            .32378016577292664623E-13,
            .81718234434207194332E-15,
            .15421338333938233722E-16,
            .21197922901636186120E-18,
            .20544296737880454267E-20,
            .13469825866373951558E-22,
            .56612941303973593711E-25,
            .14185605454630369059E-27,
            .19133754944542243094E-30,
            .11922487600982223565E-33,
            .26715112192401369860E-37,
            .13386169421062562827E-41,
            .45105361938989742322E-47
    };

    // Gauss-Hermite quadrature abscissas
    private static final double[] X = {.44489365833267018419E-1,
            .23452610951961853745,
            .57688462930188642649,
            .10724487538178176330E1,
            .17224087764446454411E1,
            .25283367064257948811E1,
            .34922132730219944896E1,
            .46164567697497673878E1,
            .59039585041742439466E1,
            .73581267331862411132E1,
            .89829409242125961034E1,
            .10783018632539972068E2,
            .12763697986742725115E2,
            .14931139755522557320E2,
            .17292454336715314789E2,
            .19855860940336054740E2,
            .22630889013196774489E2,
            .25628636022459247767E2,
            .28862101816323474744E2,
            .32346629153964737003E2,
            .36100494805751973804E2,
            .40145719771539441536E2,
            .44509207995754937976E2,
            .49224394987308639177E2,
            .54333721333396907333E2,
            .59892509162134018196E2,
            .65975377287935052797E2,
            .72687628090662708639E2,
            .80187446977913523067E2,
            .88735340417892398689E2,
            .98829542868283972559E2,
            .11175139809793769521E3};

    /**
     * An interface for calculating the eta function using different methods
     */
    public interface EtaFunction {
        /**
         * Evaluates the eta function at x given other parameters.
         *
         * @param x     The input
         * @param n1    Stage 1 sample size
         * @param alpha1    Error Rate
         * @param k     Number of systems
         * @return      Eta function evaluated at x
         */
        double eval(double x, int n1, double alpha1, long k);
    }

    // Approximate eta function implementation
    public static final EtaFunction eta_approx = (x, n1, alpha1, k) ->
            1. - Math.pow(1.0 - alpha1, 1.0 / (k - 1.0)) -
                    2. * la_gamma((n1 - 2.) / 2.) /
                            (la_gamma((n1 - 1.) / 2.) * Math.sqrt(Math.PI) *
                                    x * Math.pow(x * x + 1., (n1 - 2.0) / 2.));

    // Integral eta function implementation
    public static final EtaFunction eta_integral = (x, n1, alpha1, k) -> {
        double RHS = 1. - Math.pow(1.0 - alpha1, 1.0 / (k - 1.0));
        double LHS = 0.0;

        NormalDistribution normDist = new NormalDistribution(0, 1);
        ChiSquaredDistribution chisDist = new ChiSquaredDistribution(n1 - 1);
        for (int i = 0; i < 32; ++i) {
            LHS += WEX[i] * 4
                    * (1. - normDist.cumulativeProbability(x * Math.sqrt(X[i])))
                    * chisDist.density(X[i])
                    * (1. - chisDist.cumulativeProbability(X[i]));
        }
        return RHS - LHS;
    };

    // Evaluate eta function using the provided implementation
    private static double evalEta(double x, int n1, double alpha1, long k, EtaFunction f) {
        return f.eval(x, n1, alpha1, k);
    }

    // Log-gamma function
    public static double la_gamma(double x) {
        double[] p = {0.99999999999980993, 676.5203681218851, -1259.1392167224028,
                771.32342877765313, -176.61502916214059, 12.507343278686905,
                -0.13857109526572012, 9.9843695780195716e-6, 1.5056327351493116e-7};
        int g = 7;
        if (x < 0.5) return Math.PI / (Math.sin(Math.PI * x) * la_gamma(1 - x));

        x -= 1;
        double a = p[0];
        double t = x + g + 0.5;
        for (int i = 1; i < 9; ++i) {
            a += p[i] / (x + i);
        }

        return Math.sqrt(2 * Math.PI) * Math.pow(t, x + 0.5) * Math.exp(-t) * a;
    }

    // Find eta using bisection method
    public static double find_eta(int n1, double alpha1, long nSys) {
        double a = 0.0; // Lower bound
        double b = 15.0; // Upper bound
        double e = 0.0001; // Tolerance
        double dx = b - a; // Interval length
        double x = dx / 2.; // Midpoint
        int k = 0; // Iteration counter

        // Precompute weights for numerical integration
        for (int i = 1; i <= 32; ++i) {
            WEX[i - 1] = W[i - 1] * Math.exp(X[i - 1]);
        }

        // Choose the eta function implementation
        EtaFunction f = APPROX_ETA ? eta_approx : eta_integral;

        // Check if the signs of f(a) and f(b) are different
        if (((evalEta(a, n1, alpha1, nSys, f) >= 0) &&
                (evalEta(b, n1, alpha1, nSys, f) >= 0))
                || ((evalEta(a, n1, alpha1, nSys, f) < 0) &&
                (evalEta(b, n1, alpha1, nSys, f) < 0))) {
            System.out.print("\nThe values of f(a) and f(b) do not differ in sign.\n");
            return 0.0;
        }

        // Bisection method to find the root
        while (Math.abs(dx) > e && k < 1000 && evalEta(x, n1, alpha1, nSys, f) != 0) {
            x = ((a + b) / 2.);
            if ((evalEta(a, n1, alpha1, nSys, f) * evalEta(x, n1, alpha1, nSys, f)) < 0) {
                b = x;
            } else {
                a = x;
            }
            dx = b - a;
            k++;
        }
        return x;
    }
}