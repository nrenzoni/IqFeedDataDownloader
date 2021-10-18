using System;
using CustomShared;
using NodaTime;

namespace IqFeedDownloaderLib
{
    public class DownloadDateSchema
    {
        public LocalDate StartDate { get; set; }
        public LocalDate EndDate { get; set; }

        public LocalDate SingleDate
        {
            get
            {
                if (!IsSingleDay)
                    throw new Exception($"{nameof(SingleDate)} can only be called " +
                                        $"if {nameof(DownloadDateSchema)} obj only contains a single date.");

                return StartDate;
            }
        }

        public DownloadDateSchema(LocalDate startDate, LocalDate? endDate = null)
        {
            StartDate = startDate;
            EndDate = endDate ?? StartDate;
        }

        public bool IsSingleDay => StartDate == EndDate;

        public override string ToString() =>
            IsSingleDay
                ? StartDate.ToYYYYMMDD()
                : $"[{StartDate.ToYYYYMMDD()}, {EndDate.ToYYYYMMDD()}]";

        public bool ContainsDate(LocalDate date)
        {
            if (IsSingleDay)
                return StartDate == date;
            return StartDate <= date && date <= EndDate;
        }
    }
}