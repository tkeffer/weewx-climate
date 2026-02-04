<div id="climate_widget" class="widget">
  <div class="widget_title">
F    <a href="climate.html">$gettext("On this day")</a>
  </div>
  <div class="widget_content" style="display:block;">
    <table style="width:100%;">
      <tbody>
        <tr>
          <td class="label">$gettext("All-Time High")</td>
          <td class="data" style="padding-left:10px;"><b>$climate.day.outTemp.high.max</b></td>
          <td class="timestamp" style="vertical-align:bottom">($climate.day.outTemp.high.maxtime)</td>
        </tr>
        <tr>
          <td class="label">$gettext("Lowest High")</td>
          <td class="data" style="padding-left:10px;"><b>$climate.day.outTemp.high.min</b></td>
          <td class="timestamp" style="vertical-align:bottom">($climate.day.outTemp.high.mintime)</td>
        </tr>
        <tr>
          <td class="label">$gettext("Average High")</td>
          <td class="data" style="padding-left:10px;"><b>$climate.day.outTemp.high.avg</b></td>
          <td class="timestamp" class="timestamp">&nbsp;</td>
        </tr>
        <tr>
          <td class="label">$gettext("All-Time Low")</td>
          <td class="data" style="padding-left:10px;"><b>$climate.day.outTemp.low.min</b></td>
          <td class="timestamp" style="vertical-align:bottom">($climate.day.outTemp.low.mintime)</td>
        </tr>
        <tr>
          <td class="label">$gettext("Highest Low")</td>
          <td class="data" style="padding-left:10px;"><b>$climate.day.outTemp.low.max</b></td>
          <td class="timestamp" style="vertical-align:bottom">($climate.day.outTemp.low.maxtime)</td>
        </tr>
        <tr>
          <td class="label">$gettext("Average Low")</td>
          <td class="data" style="padding-left:10px;"><b>$climate.day.outTemp.low.avg</b></td>
          <td class="timestamp">&nbsp;</td>
        </tr>
        <tr>
          <td class="label">$gettext("Max Precipitation")</td>
          <td class="data" style="padding-left:10px;"><b>$climate.day.precip.sum.max</b></td>
          <td class="timestamp" style="vertical-align:bottom">($climate.day.precip.sum.maxtime)</td>
        </tr>
      </tbody>
    </table>
  </div>
</div>