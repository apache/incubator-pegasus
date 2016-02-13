using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Threading;

namespace rDSN.Tron.Utility
{
    class ExpDelay : Singleton<ExpDelay>
    {
        public const int DELAY_COUNT = 6;
        public double[] _defaultDelayPoints;
        public int[] _defaultDelay; // millieseconds

        private double _prob;
        private int[] _delay = new int[DELAY_COUNT];

        public ExpDelay()
        {
            _prob = 0.9;
            _defaultDelayPoints = new double[] { 1.0, 1.2, 1.4, 1.6, 1.8, 2.0 };
            _defaultDelay = new int[] { 0, 0, 1, 2, 5, 10 }; // millieseconds
            Initialize(_defaultDelay, _prob);
        }

        public void Initialize(int[] delays, double prob)
        {
            Trace.Assert(delays.Length == DELAY_COUNT);
            for (int i = 0; i < DELAY_COUNT; i++)
            {
                _delay[i] = delays[i];
            }
            _prob = prob;
        }

        public static int GetDelayInterval(long value, long threshhold)
        {
            return Instance().DelayInternal(value, threshhold);
        }

        public static void Delay(long value, long threshold)
        {
            int delay = GetDelayInterval(value, threshold);
            if (delay > 0) Thread.Sleep(delay);
        }

        private int DelayInternal(long value, long threshhold)
        {
            if (value >= threshhold && (double)_ran.Next(0, 100) / 100.0 < _prob)
            {
                double f = (double)value / (double)threshhold;
                if (f < _defaultDelayPoints[DELAY_COUNT - 1])
                {
                    int idx = (int)((f - 1.0) / 0.2);
                    return _delay[idx];
                }
                else
                {
                    return _delay[DELAY_COUNT - 1];
                }
            }

            return 0;
        }

        private Random _ran = new Random();
    }
}
