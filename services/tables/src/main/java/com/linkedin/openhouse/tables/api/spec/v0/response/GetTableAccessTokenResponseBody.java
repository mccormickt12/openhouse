package com.linkedin.openhouse.tables.api.spec.v0.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;

@Builder(toBuilder = true)
@Value
public class GetTableAccessTokenResponseBody {

  @Schema(description = "JWT Access Token String", example = "header.payload.signature")
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  private String accessToken;

  public String toJson() {
    return new Gson().toJson(this);
  }
}
