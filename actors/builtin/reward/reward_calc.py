#!/usr/bin/python3

# This file can be used to recalculate the reward constants in the code when
# changing the reward factor and/or the epoch duration in seconds.

from decimal import Decimal
import decimal

# Number of seconds per epoch.
EPOCH_DURATION_SECONDS = 30

# Growth factor per year. Currently 200%.
GROWTH_FACTOR = 2

# Precision factor.
Q128 = 2**128

# Set the precision to enough digits to store (Q128 * Q128).
#
# This gives us wolfram-alpha level precision.
decimal.getcontext().prec=int(Decimal(Q128**2).log10().to_integral_value(decimal.ROUND_CEILING))

def epochs_in_year() -> Decimal:
    return Decimal(60*60*365*24)/Decimal(EPOCH_DURATION_SECONDS)

def q128(val, round) -> str:
    return str((Q128 * val).to_integral_value(round))

# exp(ln[1 + 200%] / epochsInYear)
def baseline_exponent() -> Decimal: 
    return (Decimal(1 + GROWTH_FACTOR).ln() / epochs_in_year()).exp()

def baseline_exponent_q128() -> str: 
    return q128(baseline_exponent(), decimal.ROUND_HALF_UP)

# ln(2) / (6 * epochsInYear)
def reward_lambda() -> Decimal: 
    # 2 is a constant such that the half life is 6 years.
    return Decimal(2).ln() / (6 * epochs_in_year())

def reward_lambda_q128() -> str: 
    return q128(reward_lambda(), decimal.ROUND_FLOOR)

# exp(lambda) - 1
def reward_lambda_prime() -> Decimal: 
    return reward_lambda().exp() - 1

def reward_lambda_prime_q128() -> str: 
    return q128(reward_lambda_prime(), decimal.ROUND_FLOOR)

def main():
    print("BaselineExponent: ", baseline_exponent_q128())
    print("lambda: ", reward_lambda_q128())
    print("expLamSubOne: ", reward_lambda_prime_q128())

if __name__ == "__main__":
    main()
