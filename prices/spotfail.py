#!/bin/bash
# Failure inter-arrival times distribution
import os,sys,datetime,time,dateutil.parser
import statsmodels.discrete
import scipy.stats
import matplotlib.pyplot as plt


def inter_arrival_times(trace_file):
    BID=0.06 #On-demand price
    first_failure = 0 #Time of first failure. Maybe ignore it?    
    f = open(trace_file, 'r')
    lines = f.readlines()
    pricetrace = parse_lines(lines) #(time, price tuples)
    failures = get_failures(pricetrace, BID)    
    faildist = to_dist(failures)


def to_dist(failures):
    if len(failures) == 0 :
        return []
    
    mle = sum(failures)/float(len(failures))
    print "MLE lambda= "+str(mle)
    dis=scipy.stats.expon.fit(failures)
    print "Scipy mu, sigma= "+str(dis)
    ks = scipy.stats.kstest(failures,'expon')
    print "Kolmogorov smirnov= "+str(ks)
    print "----------------------------------"
    #statsmodels.discrete.discrete_model.Poisson.fit
    plot_stuff(failures,dis)

def plot_stuff(f,dis,plot=True,pickle=False):
    num_bins = 20 #who really knows?
    h = plt.hist(f,num_bins,normed=True,color='black')
    size=max(f)
    dist_names = ['expon', 'pareto'] #, 'gamma', 'beta', 'rayleigh', 'norm', 'pareto']
    x = scipy.arange(size)
    for dist_name in dist_names:
        dist = getattr(scipy.stats, dist_name)
        param = dist.fit(f)
        print "Param for "+dist_name+": "+str(param)
        #Right place to pickle f, filename, dist_name, param
        pdf_fitted = dist.pdf(x, *param[:-2], loc=param[-2], scale=param[-1]) * size
        plt.plot(pdf_fitted, label=dist_name)

    
    print "Plot:"
    plt.show()
    
def parse_lines(lines):
    #Input: list of strings
    #Input Format: 2012-09-11 09:41:13,0.106
    #Output:list of [(unix-time-seconds, price)]
    pricetrace = []
    for aline in lines:
        #parsing exceptions and all that?
        try:
            (tstamp,price) = tuple(aline.split(","))
            price = float(price)
            dt = dateutil.parser.parse(tstamp)
            tstamp = time.mktime(dt.timetuple())
            pricetrace.append((tstamp, price))
        except:
            pass
    return pricetrace


def get_failures(pricetrace, bid):
    #Input: [(unixtime, price)]
    #Output: [inter-arrival-times]
    out = []
    firstfail = True
    prevfail = 0
    timebuf = 600 #10 minutes
    for (t,p) in pricetrace:
        if p > bid :
            if firstfail:
                prevfail = t
                firstfail = False
            else :
                iat = t - prevfail
                prevfail = t
                if iat < timebuf:
                    #ignore it, do not append!
                    pass
                else:
                    out.append(iat)

    print "Number of failures: "+str(len(out))
    return out       

def all_iat(d):
    for f in os.listdir(d):
        fp = os.path.join(d,f)
        print fp
        inter_arrival_times(fp)



#all_iat(sys.argv[1])

inter_arrival_times(sys.argv[1])
