package com.ebiznext.comet.schema.model.atlas

object AtlasData {

  val domainContent: String =
    """
      |---
      |name: mone
      |directory: /tmp/incoming/mone
      |ack: ""
      |metadata:
      |  mode: FILE
      |  withHeader: false
      |  encoding: ISO-8859-1
      |  format: POSITION
      |  sink:
      |    type: ES
      |  write: OVERWRITE
      |  partition:
      |    sampling: 0.5
      |    attributes:
      |      - comet_year
      |      - comet_month
      |      - comet_day
      |schemas:
      |  - name: DWMMA24_DE_PAYS
      |    pattern: DWMMA24_DE_PAYS.csv
      |    attributes:
      |      - name: COPAYN
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment: Code Pays Numérique
      |        position:
      |          first: 8
      |          last: 10
      |          trim: NONE
      |      - name: COPAYS
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment: Code Pays
      |        position:
      |          first: 32
      |          last: 34
      |          trim: NONE
      |      - name: COPAYA
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment: Code Pays sur 2 carac
      |        position:
      |          first: 35
      |          last: 36
      |          trim: NONE
      |      - name: CEPAEU
      |        type: byte
      |        required: true
      |        privacy: NONE
      |        comment: Zone Euro
      |        position:
      |          first: 37
      |          last: 37
      |          trim: NONE
      |      - name: LIPAYS
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment: Libellé Code Pays
      |        position:
      |          first: 38
      |          last: 87
      |          trim: RIGHT
      |      - name: CODCB
      |        type: byte
      |        required: true
      |        privacy: NONE
      |        comment: Indicateur pays CB O/N
      |        position:
      |          first: 88
      |          last: 88
      |      - name: COVISA
      |        type: byte
      |        required: true
      |        privacy: NONE
      |        comment: Indicateur pays VISA O/N
      |        position:
      |          first: 89
      |          last: 89
      |      - name: CODMCI
      |        type: byte
      |        required: true
      |        privacy: NONE
      |        comment: Indicateur pays MCI O/N
      |        position:
      |          first: 90
      |          last: 90
      |      - name: CEPMCI
      |        type: byte
      |        required: true
      |        privacy: NONE
      |        comment: Zone euro MCI
      |        position:
      |          first: 91
      |          last: 91
      |      - name: FGDOMI
      |        type: byte
      |        required: true
      |        privacy: NONE
      |        comment: ZONE DOMESTIQUE INTERNATIONALE
      |        position:
      |          first: 92
      |          last: 92
      |  - name: DWMMA41_DE_TYPE_CARTE
      |    pattern: DWMMA41_DE_TYPE_CARTE.csv
      |    attributes:
      |      - name: CODCDF
      |        type: byte
      |        required: true
      |        privacy: NONE
      |        comment: Code chef de file
      |        position:
      |          first: 8
      |          last: 8
      |      - name: COETCF
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment: Code établissement chef de file
      |        position:
      |          first: 9
      |          last: 13
      |          trim: RIGHT
      |      - name: CTCAR
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment: Type de carte
      |        position:
      |          first: 14
      |          last: 16
      |          trim: NONE
      |      - name: LICAR
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment: Libellé type de carte
      |        position:
      |          first: 32
      |          last: 66
      |          trim: NONE
      |      - name: LICAMK
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment: Libellé type de carte Marketing
      |        position:
      |          first: 67
      |          last: 116
      |          trim: RIGHT
      |      - name: LIERMK
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment: Libellé émetteur réseau Marketing
      |        position:
      |          first: 117
      |          last: 131
      |          trim: RIGHT
      |      - name: LICLMK
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment: Libellé type de client Marketing
      |        position:
      |          first: 132
      |          last: 146
      |          trim: RIGHT
      |      - name: RGTSTAT
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment: Regroupement statistique
      |        position:
      |          first: 147
      |          last: 176
      |          trim: RIGHT
      |  - name: DWMMA52_SC_FACT_CLIENT
      |    metadata:
      |      format: DSV
      |      encoding: UTF-8
      |      separator: µ
      |    pattern: DWMMA52_SC_FACT_CLIENT.csv
      |    attributes:
      |      - name: IDMETI
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment: Identifiant métier
      |      - name: CODAPP
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment: Code application MONETIQUE
      |      - name: DADEVA
      |        type: date_fr
      |        required: true
      |        privacy: NONE
      |        comment: Date de début de validité client
      |      - name: DAFIVA
      |        type: date_fr
      |        required: true
      |        privacy: NONE
      |        comment: Date de fin de validité client
      |      - name: COBQAP
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment: Code banque d'appartenance
      |      - name: COGRPE
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment: Code groupe d'appartenance
      |      - name: IDCLIE
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment: Identifiant client facturation
      |      - name: LIBCLI
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment: Libellé client facturation
      |  - name: DWMMA53_SC_FACT_QUAFLUX
      |    metadata:
      |      format: DSV
      |      encoding: UTF-8
      |      separator: µ
      |    pattern: DWMMA53_SC_FACT_QUAFLUX.csv
      |    attributes:
      |      - name: COQUAL
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment: Qualifiant de flux facturation
      |      - name: COGRP1
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment: Code groupe d'appartenance 1
      |      - name: COGRP2
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment: Code groupe d'appartenance 2
      |      - name: TOPBQE
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment: Top banques identiques
      |      - name: DADEVA
      |        type: date_fr
      |        required: true
      |        privacy: NONE
      |        comment: Date de début de validité du qualifiant
      |      - name: DAFIVA
      |        type: date_fr
      |        required: true
      |        privacy: NONE
      |        comment: Date de fin de validité du qualifiant
      |  - name: DWMMADA_SC_SMARTPORTEUR
      |    pattern: DWMMADA_SC_SMARTPORTEUR.csv
      |    attributes:
      |      - name: CTENR
      |        type: byte
      |        required: true
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 0
      |          last: 0
      |      - name: COECFM
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 1
      |          last: 5
      |          trim: NONE
      |      - name: COBQRE
      |        type: string
      |        required: true
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 6
      |          last: 10
      |          trim: NONE
      |      - name: COGUIC
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 11
      |          last: 15
      |          trim: NONE
      |      - name: COCART
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 16
      |          last: 34
      |          trim: NONE
      |      - name: CTQUAP
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          trim: RIGHT
      |          first: 35
      |          last: 37
      |      - name: LIPPO
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          trim: RIGHT
      |          first: 38
      |          last: 69
      |      - name: LIPRP
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 70
      |          last: 101
      |          trim: RIGHT
      |      - name: DANAIS
      |        type: date_fr2
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 102
      |          last: 111
      |          trim: NONE
      |      - name: CTSXC
      |        type: byte
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 112
      |          last: 112
      |      - name: LIEPO
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 113
      |          last: 144
      |          trim: RIGHT
      |      - name: COTYCL
      |        type: byte
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 145
      |          last: 145
      |      - name: CORIBC
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 146
      |          last: 168
      |      - name: CORIBE
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 169
      |          last: 191
      |          trim: RIGHT
      |      - name: LIADR1
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 192
      |          last: 223
      |          trim: RIGHT
      |      - name: LIADR2
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 224
      |          last: 255
      |          trim: RIGHT
      |      - name: LIADR3
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 256
      |          last: 287
      |          trim: RIGHT
      |      - name: LIADR4
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 288
      |          last: 319
      |          trim: RIGHT
      |      - name: COPSTA
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 320
      |          last: 324
      |          trim: NONE
      |      - name: LIBUD
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 325
      |          last: 351
      |          trim: RIGHT
      |      - name: COPAYS
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 352
      |          last: 354
      |          trim: NONE
      |      - name: DATADH
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 355
      |          last: 364
      |          trim: NONE
      |      - name: CTVISU
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 365
      |          last: 367
      |          trim: NONE
      |      - name: CECAGU
      |        type: byte
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 368
      |          last: 368
      |          trim: NONE
      |      - name: CECAR
      |        type: byte
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 369
      |          last: 369
      |          trim: NONE
      |      - name: DDVACA
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 370
      |          last: 375
      |          trim: NONE
      |      - name: DFVACA
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 376
      |          last: 381
      |          trim: NONE
      |      - name: COCAM
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 382
      |          last: 400
      |          trim: RIGHT
      |      - name: CORCA
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 401
      |          last: 419
      |          trim: NONE
      |      - name: COPPOR
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 420
      |          last: 434
      |          trim: RIGHT
      |      - name: CTDEBC
      |        type: byte
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 435
      |          last: 435
      |          trim: NONE
      |      - name: LIAL
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 436
      |          last: 467
      |          trim: NONE
      |      - name: CODOCL
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 468
      |          last: 488
      |          trim: NONE
      |      - name: CORCCZ
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 489
      |          last: 509
      |      - name: CEPRF
      |        type: byte
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 510
      |          last: 510
      |      - name: COPRF
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 511
      |          last: 526
      |      - name: CEPRFT
      |        type: byte
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 527
      |          last: 527
      |      - name: COPRFT
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 528
      |          last: 543
      |      - name: DADPRF
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 544
      |          last: 553
      |      - name: DAFPRF
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 554
      |          last: 563
      |      - name: DANCA
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 564
      |          last: 573
      |      - name: COMANL
      |        type: byte
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 574
      |          last: 574
      |      - name: DAOPC
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 575
      |          last: 584
      |      - name: COMOOS
      |        type: byte
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 585
      |          last: 585
      |      - name: COMAC
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 586
      |          last: 588
      |      - name: CEALT
      |        type: byte
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 589
      |          last: 589
      |      - name: CELATE
      |        type: byte
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 590
      |          last: 590
      |      - name: CECARS
      |        type: byte
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 591
      |          last: 591
      |      - name: DAACTI
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 592
      |          last: 601
      |      - name: CEDSAC
      |        type: byte
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 602
      |          last: 602
      |      - name: COTVI
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 603
      |          last: 606
      |      - name: LITEPO
      |        type: string
      |        required: false
      |        privacy: NONE
      |        comment:
      |        position:
      |          first: 607
      |          last: 622
      |""".stripMargin
}
