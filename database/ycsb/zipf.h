#ifndef __DATABASE_ZIPF_H__
#define __DATABASE_ZIPF_H__

#include "cmath"

class Zipf {
   public:
    void init(int n, double theta) {
        hasInit = true;

        n_ = n;
        theta_ = theta;
        alpha_ = 1.0 / (1.0 - theta_);
        zetan_ = zeta(n_);
        eta_ =
            (1.0 - std::pow(2.0 / n_, 1.0 - theta_)) / (1.0 - zeta(2) / zetan_);
    }

    int value(double u) {
        double uz = u * zetan_;
        int v;
        if (uz < 1) {
            v = 0;
        } else if (uz < 1 + std::pow(0.5, theta_)) {
            v = 1;
        } else {
            v = static_cast<int>(n_ * std::pow(eta_ * u - eta_ + 1, alpha_));
        }
        return v;
    }

    static Zipf &globalZipf() {
        static Zipf z;
        return z;
    }

   private:
    double zeta(int n) {
        double sum = 0;

        for (auto i = 1; i <= n; i++) {
            sum += std::pow(1.0 / i, theta_);
        }

        return sum;
    }

    bool hasInit = false;

    int n_;
    double theta_;
    double alpha_;
    double zetan_;
    double eta_;
};
#endif