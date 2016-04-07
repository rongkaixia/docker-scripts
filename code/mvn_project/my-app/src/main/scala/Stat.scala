// scalastyle:off println
package com.keystone.OHLCSearchEngine

import scala.math.random
import scala.reflect.ClassTag
import scala.collection.JavaConversions._

import breeze.linalg._
import breeze.math._
import breeze.numerics._
import breeze.stats.{mean, stddev}

object Stat {
    // each row of A is an observation
    // TODO: Check Normalization
    def normalization(A: DenseMatrix[Double]): DenseMatrix[Double] = {
        val _mean = mean(A(::,*)).toDenseVector
        // val _mean = mean(A(::,*)).t
        val _std = stddev(A(::,*))
        A(*,::) / _mean
    }

    def euclideanDistanceKernel(A: DenseMatrix[Double]): DenseMatrix[Double] = {
        // sum(A, 1) - 2AA^T + sum(A, 1)^T
        var ret = -2.0 * A * A.t
        val _sum2 = sum(pow(A,2),Axis._1)
        ret(*,::) += _sum2
        ret(::,*) += _sum2
        sqrt(ret)
    }

    def centerDistance(A: DenseMatrix[Double]): DenseMatrix[Double] = {
        var dist = euclideanDistanceKernel(A)
        val allMean = mean(dist)
        val rowMean = mean(dist(*,::))
        val colMean = mean(dist(::,*)).toDenseVector
        // val colMean = mean(dist(::,*)).t
        dist(*,::) -= rowMean
        dist(::,*) -= colMean
        dist += allMean
        dist
    }

    def brownianCov(A: DenseMatrix[Double], B: DenseMatrix[Double]): Double = {
        val Adist = centerDistance(euclideanDistanceKernel(A))
        val Bdist = centerDistance(euclideanDistanceKernel(B))
        sqrt(sum(Adist :* Bdist)) / Adist.rows
    }

    def brownianCorrelation(A: DenseMatrix[Double], B: DenseMatrix[Double]): Double = {
        val nA = normalization(A)
        val nB = normalization(B)
        brownianCov(nA,nB)/sqrt(brownianCov(nA,nA)*brownianCov(nB,nB))
    }

}