####################################################################
#A dplyr connector for the Vertica database.
#Copyright (C) [2015] Hewlett-Packard Development Company, L.P.

#This program is free software; you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation; either version 2 of the License, or (at
#your option) any later version.

#This program is distributed in the hope that it will be useful, but
#WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
#General Public License for more details.  You should have received a
#copy of the GNU General Public License along with this program; if
#not, write to the Free Software Foundation, Inc., 59 Temple Place,
#Suite 330, Boston, MA 02111-1307 USA
#####################################################################

#' Vertica.dplyR is an R package developed by HP that provides Vertica-backend support for the [dplyr][1] package. Besides support of standard dplyr functionality (such as table manipulation), vertica.dplyr also features:
#'
#'
#' * [HPdata][4]-style functions (data transport from Vertica to Distributed R via the data loader), namely tbl2d**object** (where **object** is either **array** or **frame**), that are compatible with dplyr tbl objects.
#' * Easy CSV-loading into Vertica.
#' * copy_to functionality that takes advantage of Vertica's fast **COPY LOCAL** feature.
#' * Connectivity to Vertica through either JDBC or ODBC.
#' * (Future Feature) Seamless Vertica UDx invocation.
#'
#' For more information, please read the PDF or HTML of the full user guide in vignettes/.
#' @docType package
#' @name vertica.dplyr
NULL
