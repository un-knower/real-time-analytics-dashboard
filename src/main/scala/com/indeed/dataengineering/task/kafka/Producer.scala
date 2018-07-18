package com.indeed.dataengineering.task.kafka


/**
  * Created by aguyyala on 10/19/17.
  */


import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import com.github.nscala_time.time.Imports.DateTime
import com.indeed.dataengineering.config.AnalyticsTaskConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.concurrent.duration._

object Producer extends App {

  val conf: Map[String, String] = AnalyticsTaskConfig.parseCmdLineArguments(args)
  val topic: String = conf("kafka.topic")
  val countries = List(("AFGHANISTAN", "KABUL"), ("ALBANIA", "TIRANA"), ("ALGERIA", "ALGIERS"), ("ANDORRA", "ANDORRA LA VELLA"), ("ANGOLA", "LUANDA"), ("ARGENTINA", "BUENOS AIRES"), ("ARMENIA", "YEREVAN"), ("AUSTRALIA", "CANBERRA"), ("AUSTRIA", "VIENNA"), ("AZERBAIJAN", "BAKU"), ("BAHAMAS, THE", "NASSAU"), ("BAHRAIN", "MANAMA"), ("BANGLADESH", "DHAKA"), ("BARBADOS", "BRIDGETOWN"), ("BELARUS", "MINSK"), ("BELGIUM", "BRUSSELS"), ("BELIZE", "BELMOPAN"), ("BENIN", "PORTO-NOVO"), ("BHUTAN", "THIMPHU"), ("BOLIVIA", "SUCRE"), ("BOSNIA & HERZEGOVINA", "SARAJEVO"), ("BOTSWANA", "GABORONE"), ("BRAZIL", "BRASILIA"), ("BRUNEI", "BANDAR SERI BEGAWAN"), ("BULGARIA", "SOFIA"), ("BURKINA FASO", "OUAGADOUGOU"), ("BURUNDI", "BUJUMBURA"), ("CABO VERDE", "PRAIA"), ("CAMBODIA", "PHNOM PENH"), ("CAMEROON", "YAOUNDE"), ("CANADA", "OTTAWA"), ("CENTRAL AFRICAN REPUBLIC", "BANGUI"), ("CHILE", "SANTIAGO"), ("CHINA", "BEIJING"), ("COLOMBIA", "BOGOTÁ"), ("COMOROS", "MORONI"), ("CONGO, DEMOCRATIC REPUBLIC OF THE", "KINSHASA"), ("COSTA RICA", "SAN JOSE"), ("CROATIA", "ZAGREB"), ("CUBA", "HAVANA"), ("CYPRUS", "NICOSIA"), ("CZECH REPUBLIC", "PRAGUE"), ("DENMARK", "COPENHAGEN"), ("DJIBOUTI", "DJIBOUTI (CITY)"), ("DOMINICA", "ROSEAU"), ("DOMINICAN REPUBLIC", "SANTO DOMINGO"), ("ECUADOR", "QUITO"), ("EGYPT", "CAIRO"), ("EL SALVADOR", "SAN SALVADOR"), ("EQUATORIAL GUINEA", "MALABO"), ("ERITREA", "ASMARA"), ("ESTONIA", "TALLINN"), ("ETHIOPIA", "ADDIS ABABA"), ("FEDERATED STATES OF MICRONESIA", "PALIKIR"), ("FIJI", "SUVA"), ("FINLAND", "HELSINKI"), ("FRANCE", "PARIS"), ("GABON", "LIBREVILLE"), ("GAMBIA, THE", "BANJUL"), ("GEORGIA", "TBILISI"), ("GERMANY", "BERLIN"), ("GHANA", "ACCRA"), ("GREECE", "ATHENS"), ("GUATEMALA", "GUATEMALA CITY"), ("GUINEA", "CONAKRY"), ("GUINEA-BISSAU", "BISSAU"), ("GUYANA", "GEORGETOWN"), ("HAITI", "PORT-AU-PRINCE"), ("HONDURAS", "TEGUCIGALPA"), ("HUNGARY", "BUDAPEST"), ("ICELAND", "REYKJAVIK"), ("INDIA", "NEW DELHI"), ("INDONESIA", "JAKARTA"), ("IRAN", "TEHRAN"), ("IRAQ", "BAGHDAD"), ("IRELAND", "DUBLIN"), ("ISRAEL", "JERUSALEM, TEL AVIV"), ("ITALY", "ROME"), ("JAMAICA", "KINGSTON"), ("JAPAN", "TOKYO"), ("JORDAN", "AMMAN"), ("KAZAKHSTAN", "ASTANA"), ("KENYA", "NAIROBI"), ("KIRIBATI", "SOUTH TARAWA"), ("KOSOVO", "PRISTINA"), ("KUWAIT", "KUWAIT CITY"), ("KYRGYZSTAN", "BISHKEK"), ("LAOS", "VIENTIANE"), ("LATVIA", "RIGA"), ("LEBANON", "BEIRUT"), ("LESOTHO", "MASERU"), ("LIBERIA", "MONROVIA"), ("LIBYA", "TRIPOLI"), ("LIECHTENSTEIN", "VADUZ"), ("LITHUANIA", "VILNIUS"), ("LUXEMBOURG", "LUXEMBOURG"), ("MACEDONIA", "SKOPJE"), ("MADAGASCAR", "ANTANANARIVO"), ("MALAWI", "LILONGWE"), ("MALAYSIA", "KUALA LUMPUR"), ("MALDIVES", "MALE"), ("MALI", "BAMAKO"), ("MALTA", "VALLETTA"), ("MARSHALL ISLANDS", "MAJURO"), ("MAURITANIA", "NOUAKCHOTT"), ("MAURITIUS", "PORT LOUIS"), ("MEXICO", "MEXICO CITY"), ("MOLDOVA", "CHISINAU"), ("MONACO", "MONACO"), ("MONGOLIA", "ULAANBAATAR"), ("MONTENEGRO", "PODGORICA"), ("MOROCCO", "RABAT"), ("MOZAMBIQUE", "MAPUTO"), ("MYANMAR", "NAY PYI TAW"), ("NAMIBIA", "WINDHOEK"), ("NAURU", "YAREN DISTRICT"), ("NEPAL", "KATHMANDU"), ("NETHERLANDS", "AMSTERDAM"), ("NEW ZEALAND", "WELLINGTON"), ("NICARAGUA", "MANAGUA"), ("NIGER", "NIAMEY"), ("NIGERIA", "ABUJA"), ("NORTH KOREA", "PYONGYANG"), ("NORWAY", "OSLO"), ("OMAN", "MUSCAT"), ("PAKISTAN", "ISLAMABAD"), ("PALAU", "NGERULMUD"), ("PALESTINE", "JERUSALEM, RAMALLAH"), ("PANAMA", "PANAMA CITY"), ("PAPUA NEW GUINEA", "PORT MORESBY"), ("PERU", "LIMA"), ("PHILIPPINES", "MANILA"), ("POLAND", "WARSAW"), ("PORTUGAL", "LISBON"), ("QATAR", "DOHA"), ("REPUBLIC OF THE CONGO", "BRAZZAVILLE"), ("ROMANIA", "BUCHAREST"), ("RUSSIA", "MOSCOW"), ("RWANDA", "KIGALI"), ("SAINT KITTS & NEVIS", "BASSETERRE"), ("SAINT LUCIA", "CASTRIES"), ("SAINT VINCENT & THE GRENADINES", "KINGSTOWN"), ("SAMOA", "APIA"), ("SAN MARINO", "SAN MARINO"), ("SAUDI ARABIA", "RIYADH"), ("SENEGAL", "DAKAR"), ("SERBIA", "BELGRADE"), ("SEYCHELLES", "VICTORIA"), ("SIERRA LEONE", "FREETOWN"), ("SINGAPORE", "SINGAPORE"), ("SLOVAKIA", "BRATISLAVA"), ("SLOVENIA", "LJUBLJANA"), ("SOLOMON ISLANDS", "HONIARA"), ("SOMALIA", "MOGADISHU"), ("SOUTH AFRICA", "BLOEMFONTEIN, CAPE TOWN, PRETORIA"), ("SOUTH KOREA", "SEOUL"), ("SOUTH SUDAN", "JUBA"), ("SPAIN", "MADRID"), ("SRI LANKA", "COLOMBO, SRI JAYAWARDENEPURA KOTTE"), ("SUDAN", "KHARTOUM"), ("SURINAME", "PARAMARIBO"), ("SWAZILAND", "MBABANE"), ("SWEDEN", "STOCKHOLM"), ("SWITZERLAND", "BERN"), ("SYRIA", "DAMASCUS"), ("TAJIKISTAN", "DUSHANBE"), ("TANZANIA", "DODOMA"), ("THAILAND", "BANGKOK"), ("TIMOR-LESTE", "DILI"), ("TRINIDAD & TOBAGO", "PORT OF SPAIN"), ("TUNISIA", "TUNIS"), ("TURKEY", "ANKARA"), ("TURKMENISTAN", "ASHGABAT"), ("TUVALU", "FUNAFUTI"), ("UGANDA", "KAMPALA"), ("UKRAINE", "KIEV"), ("UNITED ARAB EMIRATES", "ABU DHABI"), ("UNITED KINGDOM", "LONDON"), ("UNITED STATES", "WASHINGTON, D.C."), ("URUGUAY", "MONTEVIDEO"), ("UZBEKISTAN", "TASHKENT"), ("VANUATU", "PORT VILA"), ("VATICAN CITY", "VATICAN CITY"), ("VENEZUELA", "CARACAS"), ("VIETNAM", "HANOI"), ("ZAMBIA", "LUSAKA"), ("ZIMBABWE", "HARARE"))

  val start = Timestamp.valueOf("2017-01-01 00:00:00").getTime
  val end = Timestamp.valueOf("2018-07-31 00:00:00").getTime
  val diff = end - start + 1

  val props = new Properties()
  props.put("bootstrap.servers", conf("kafka.brokers"))
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


  val producer = new KafkaProducer[String, String](props)

  val rand = scala.util.Random
  val runFor = conf.getOrElse("runFor", "10").toInt
  val recordsPerRun = conf.getOrElse("recordsPerRun", "10").toInt
  val sleepTime = conf.getOrElse("sleepTime", "1000").toInt

  val deadline = runFor.seconds.fromNow
  var cnt = 10
  while (cnt > 0) {
    for (_ <- 1 to recordsPerRun) {
      val randTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Timestamp(start + (Math.random * diff).toLong))
      val currentTimestamp = DateTime.now.toString("yyyy-MM-dd HH:mm:ss")

      val timestamp = if (conf.getOrElse("randTime", "false") == "true") randTime else currentTimestamp

      val (country, city) = countries(rand.nextInt(countries.length))
      val record = new ProducerRecord[String, String](topic, s"$timestamp,$country,$city")
      producer.send(record).get
    }

    Thread.sleep(sleepTime)
    cnt -= 1
  }

  cleanup()

  def cleanup(): Unit = {
    producer.close()
  }
}
